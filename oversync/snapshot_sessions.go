package oversync

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type SnapshotSessionNotFoundError struct {
	SnapshotID string
}

func (e *SnapshotSessionNotFoundError) Error() string {
	return fmt.Sprintf("snapshot session %s was not found", e.SnapshotID)
}

type SnapshotSessionExpiredError struct {
	SnapshotID string
}

func (e *SnapshotSessionExpiredError) Error() string {
	return fmt.Sprintf("snapshot session %s has expired; start a new snapshot session", e.SnapshotID)
}

type SnapshotChunkInvalidError struct {
	Message string
}

func (e *SnapshotChunkInvalidError) Error() string {
	return e.Message
}

type SnapshotSessionForbiddenError struct {
	SnapshotID string
}

func (e *SnapshotSessionForbiddenError) Error() string {
	return fmt.Sprintf("snapshot session %s does not belong to the authenticated user", e.SnapshotID)
}

type SnapshotSessionLimitExceededError struct {
	Dimension string
	Actual    int64
	Limit     int64
}

func (e *SnapshotSessionLimitExceededError) Error() string {
	return fmt.Sprintf("snapshot session %s %d exceeds limit %d", e.Dimension, e.Actual, e.Limit)
}

type snapshotMaterializedRow struct {
	tableID     int32
	keyBytes    []byte
	bundleSeq   int64
	payloadWire []byte
	rowOrdinal  int64
}

type snapshotLiveState struct {
	tableID   int32
	keyBytes  []byte
	bundleSeq int64
}

func snapshotLogicalRowKey(tableID int32, keyBytes []byte) string {
	key := appendInt32BigEndian(nil, tableID)
	key = append(key, keyBytes...)
	return string(key)
}

func (s *SyncService) CreateSnapshotSession(ctx context.Context, actor Actor) (_ *SnapshotSession, err error) {
	return s.CreateSnapshotSessionWithRequest(ctx, actor, nil)
}

// Batch 1 / Batch 2 实现。
//
// Batch 1 — 全部读查询（1 次往返）:
//
//	[0] DELETE cleanup                     → readBr.Exec()    CommandTag，丢弃
//	[1] SELECT scope_state (JOIN user)     → readBr.Query()   检查 state == INITIALIZED
//	[2] SELECT user_state                  → readBr.Query()   提取 userPK/nextBundleSeq
//	[3] SELECT row_state (JOIN user)       → readBr.Query()   建 liveRowState map
//	[4..N] 20× 表 SELECT                   → readBr.Query()   逐表读取数据
//
// Batch 2 — 全部写查询（1 次往返）:
//
//	[0] INSERT snapshot_sessions           → readBr.Exec()    CommandTag
//	[1..N] INSERT snapshot_session_rows    → readBr.Exec()    CommandTag
func (s *SyncService) CreateSnapshotSessionWithRequest(ctx context.Context, actor Actor, req *SnapshotSessionCreateRequest) (_ *SnapshotSession, err error) {
	done, err := s.beginOperation()
	if err != nil {
		return nil, err
	}
	defer done()
	if err := actor.validate(false); err != nil {
		return nil, err
	}

	var resp *SnapshotSession
	err = pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead}, func(tx pgx.Tx) error {
		// Phase 1: Batch 1 — 全部读查询
		readBatch, tableInfos := buildSnapshotReadBatch(actor.UserID, s.registeredTableByID)
		readBr := tx.SendBatch(ctx, readBatch)

		// Phase 2: 从 Batch 1 逐结果读取 + 验证
		snapshotBundleSeq, rows, byteCount, rowErr := s.processSnapshotReadBatch(readBr, tableInfos, actor.UserID)
		if err := readBr.Close(); err != nil {
			if rowErr != nil {
				return rowErr
			}
			return fmt.Errorf("close read batch: %w", err)
		}
		if rowErr != nil {
			return rowErr
		}

		rowCount := int64(len(rows))
		if limit := s.maxRowsPerSnapshotSession(); limit > 0 && rowCount > limit {
			return &SnapshotSessionLimitExceededError{Dimension: "row_count", Actual: rowCount, Limit: limit}
		}
		if limit := s.maxBytesPerSnapshotSession(); limit > 0 && byteCount > limit {
			return &SnapshotSessionLimitExceededError{Dimension: "byte_count", Actual: byteCount, Limit: limit}
		}

		// Phase 3: 源替换（当前 req 始终 nil，不触发）
		if req != nil && req.SourceReplacement != nil {
			return fmt.Errorf("source replacement not supported in batched snapshot path")
		}

		// Phase 4: Batch 2 — 写入
		snapshotID := uuid.NewString()
		expiresAt := time.Now().UTC().Add(s.snapshotSessionTTL())
			if _, err := tx.Exec(ctx, `
				INSERT INTO sync.snapshot_sessions (
					snapshot_id, user_pk, snapshot_bundle_seq, row_count, byte_count, expires_at
				) VALUES ($1::uuid, (SELECT user_pk FROM sync.user_state WHERE user_id = $2), $3, $4, $5, $6)
			`, snapshotID, actor.UserID, snapshotBundleSeq, rowCount, byteCount, expiresAt); err != nil {
				return fmt.Errorf("insert snapshot session: %w", err)
			}

		insertBr := tx.SendBatch(ctx, buildInsertBatch(snapshotID, rows))
		if err := execInsertBatch(insertBr, rowCount); err != nil {
			insertBr.Close()
			return err
		}

		resp = &SnapshotSession{
			SnapshotID:        snapshotID,
			SnapshotBundleSeq: snapshotBundleSeq,
			RowCount:          rowCount,
			ByteCount:         byteCount,
			ExpiresAt:         expiresAt.Format(time.RFC3339Nano),
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// processSnapshotReadBatch 从 Batch 1 逐结果读取并验证。
// 返回结果：snapshotBundleSeq, rows, byteCount, error.
func (s *SyncService) processSnapshotReadBatch(
	readBr pgx.BatchResults,
	tableInfos []registeredTableRuntimeInfo,
	userID string,
) (int64, []snapshotMaterializedRow, int64, error) {
	// [0] DELETE cleanup — 丢弃
	if _, err := readBr.Exec(); err != nil {
		return 0, nil, 0, fmt.Errorf("cleanup expired sessions: %w", err)
	}

	// [1] SELECT scope_state
	state, _, err := readScopeState(readBr)
	if err != nil {
		return 0, nil, 0, fmt.Errorf("read scope state: %w", err)
	}
	if state != scopeStateInitialized {
		return 0, nil, 0, &ScopeUninitializedError{UserID: userID}
	}

	// [2] SELECT user_state
	retained, err := readRetainedState(readBr)
	if err != nil {
		return 0, nil, 0, fmt.Errorf("read retained state: %w", err)
	}
	if retained == nil {
		return 0, nil, 0, fmt.Errorf("missing retained history state for %q", userID)
	}

	// [3] SELECT row_state (JOIN)
	liveRowState, err := readLiveRowState(readBr)
	if err != nil {
		return 0, nil, 0, fmt.Errorf("read live row state: %w", err)
	}

	// [4..N] 各表数据
	seen := make(map[string]struct{}, len(liveRowState))
	rows := make([]snapshotMaterializedRow, 0, len(liveRowState))
	var byteCount int64
	for _, info := range tableInfos {
		if err := readBatchTableRows(readBr, &info, liveRowState, seen, &rows, &byteCount, s); err != nil {
			return 0, nil, 0, fmt.Errorf("read batch rows for %s.%s: %w", info.schemaName, info.tableName, err)
		}
	}

	// 验证：row_state 中的每一行都应在业务表中存在
	for logicalKey, state := range liveRowState {
		if _, ok := seen[logicalKey]; ok {
			continue
		}
		info, err := s.tableInfoForID(state.tableID)
		if err != nil {
			return 0, nil, 0, err
		}
		key, err := wireSyncKeyFromBytes(info, state.keyBytes)
		if err != nil {
			return 0, nil, 0, fmt.Errorf("decode missing live snapshot key for table_id %d: %w", state.tableID, err)
		}
		return 0, nil, 0, fmt.Errorf("sync.row_state row %s.%s %v is missing a live business-table row", info.schemaName, info.tableName, key)
	}

	// 排序 + 赋 ordinals
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].tableID != rows[j].tableID {
			return rows[i].tableID < rows[j].tableID
		}
		return bytes.Compare(rows[i].keyBytes, rows[j].keyBytes) < 0
	})
	for i := range rows {
		rows[i].rowOrdinal = int64(i + 1)
	}

	return retained.highestBundleSeq(), rows, byteCount, nil
}

// buildSnapshotReadBatch 构建 Batch 1 — 全部读查询。
func buildSnapshotReadBatch(userID string, registeredByID map[int32]registeredTableRuntimeInfo) (*pgx.Batch, []registeredTableRuntimeInfo) {
	batch := &pgx.Batch{}

	// [0] DELETE cleanup
	batch.Queue(`DELETE FROM sync.snapshot_sessions WHERE expires_at <= now()`)

	// [1] SELECT scope_state
	batch.Queue(`
		SELECT ss.state_code, ss.lease_expires_at
		FROM sync.scope_state ss
		JOIN sync.user_state us ON us.user_pk = ss.user_pk
		WHERE us.user_id = $1
	`, userID)

	// [2] SELECT user_state
	batch.Queue(`
		SELECT user_pk, next_bundle_seq, retained_bundle_floor
		FROM sync.user_state
		WHERE user_id = $1
	`, userID)

	// [3] SELECT row_state (JOIN user_state 后使用 userID，消除 userPK 依赖)
	batch.Queue(`
		SELECT rs.table_id, rs.key_bytes, rs.bundle_seq
		FROM sync.row_state rs
		JOIN sync.user_state us ON us.user_pk = rs.user_pk
		WHERE us.user_id = $1 AND rs.deleted = FALSE
	`, userID)

	// [4..N] 各表 SELECT
	tableInfos := sortedTableInfos(registeredByID)
	for _, info := range tableInfos {
		batch.Queue(buildTableSnapshotQuery(info), userID)
	}
	return batch, tableInfos
}

// readScopeState 从 Batch 1 结果 [1] 读取 scope_state。
func readScopeState(br pgx.BatchResults) (string, *time.Time, error) {
	var (
		stateCode      int16
		leaseExpiresAt sql.NullTime
	)
	row := br.QueryRow()
	if err := row.Scan(&stateCode, &leaseExpiresAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return scopeStateUninitialized, nil, nil
		}
		return "", nil, fmt.Errorf("scan scope state: %w", err)
	}
	state, err := scopeStateNameFromCode(stateCode)
	if err != nil {
		return "", nil, err
	}
	if leaseExpiresAt.Valid {
		ts := leaseExpiresAt.Time.UTC()
		return state, &ts, nil
	}
	return state, nil, nil
}

// readRetainedState 从 Batch 1 结果 [2] 读取 user_state。
// 复用 retention.go 的 scanRetainedHistoryState。
func readRetainedState(br pgx.BatchResults) (*retainedHistoryState, error) {
	row := br.QueryRow()
	return scanRetainedHistoryState(row)
}

// readLiveRowState 从 Batch 1 结果 [3] 读取 row_state。
func readLiveRowState(br pgx.BatchResults) (map[string]snapshotLiveState, error) {
	rows, err := br.Query()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	liveState := make(map[string]snapshotLiveState)
	for rows.Next() {
		var state snapshotLiveState
		if err := rows.Scan(&state.tableID, &state.keyBytes, &state.bundleSeq); err != nil {
			return nil, fmt.Errorf("scan live snapshot row_state: %w", err)
		}
		state.keyBytes = append([]byte(nil), state.keyBytes...)
		liveState[snapshotLogicalRowKey(state.tableID, state.keyBytes)] = state
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate live snapshot row_state: %w", err)
	}
	return liveState, nil
}

// buildInsertBatch 构建 Batch 2 — 全部写查询。
//
//	[0] INSERT sync.snapshot_sessions (占位, execInsertBatch 时会跳过)
//	[1..N] INSERT sync.snapshot_session_rows
//
// execInsertBatch 负责执行并忽略第一个结果。
func buildInsertBatch(snapshotID string, rows []snapshotMaterializedRow) *pgx.Batch {
	batch := &pgx.Batch{}
	// [0] 占位 — execInsertBatch 会读取第一个 Exec 结果但不使用
	batch.Queue("SELECT 1")
	for _, row := range rows {
		batch.Queue(`
			INSERT INTO sync.snapshot_session_rows (
				snapshot_id, row_ordinal, table_id, key_bytes, bundle_seq, payload_wire
			) VALUES ($1::uuid, $2, $3, $4, $5, $6)
		`, snapshotID, row.rowOrdinal, row.tableID, row.keyBytes, row.bundleSeq, string(row.payloadWire))
	}
	return batch
}

// execInsertBatch 执行 Batch 2。
// 跳过 [0] 占位查询，然后逐条执行 INSERT。
func execInsertBatch(br pgx.BatchResults, rowCount int64) error {
	// 跳过 [0] 占位
	if _, err := br.Exec(); err != nil {
		br.Close()
		return fmt.Errorf("exec insert batch placeholder: %w", err)
	}
	// [1..N] INSERT 结果
	for i := int64(0); i < rowCount; i++ {
		if _, err := br.Exec(); err != nil {
			br.Close()
			return fmt.Errorf("insert snapshot session row %d: %w", i+1, err)
		}
	}
	return br.Close()
}

func validateSnapshotSourceReplacement(actor Actor, replacement *SnapshotSourceReplacement) (*SnapshotSourceReplacement, error) {
	if replacement == nil {
		return nil, nil
	}
	clean := &SnapshotSourceReplacement{
		PreviousSourceID: strings.TrimSpace(replacement.PreviousSourceID),
		NewSourceID:      strings.TrimSpace(replacement.NewSourceID),
		Reason:           strings.TrimSpace(replacement.Reason),
	}
	if clean.PreviousSourceID != actor.SourceID {
		return nil, &SnapshotSessionInvalidError{Message: "previous_source_id must match authenticated source_id"}
	}
	if clean.NewSourceID == "" {
		return nil, &SnapshotSessionInvalidError{Message: "new_source_id is required for source replacement"}
	}
	if clean.NewSourceID == clean.PreviousSourceID {
		return nil, &SnapshotSessionInvalidError{Message: "new_source_id must differ from previous_source_id"}
	}
	switch clean.Reason {
	case "history_pruned", "source_sequence_out_of_order", "source_sequence_changed", "source_retired":
	default:
		return nil, &SnapshotSessionInvalidError{Message: "source replacement reason is unsupported"}
	}
	return clean, nil
}

func (s *SyncService) applySnapshotSourceReplacementInTx(ctx context.Context, tx pgx.Tx, actor Actor, userPK int64, req *SnapshotSessionCreateRequest) error {
	if req == nil || req.SourceReplacement == nil {
		return nil
	}
	replacement, err := validateSnapshotSourceReplacement(actor, req.SourceReplacement)
	if err != nil {
		return err
	}

	previousState, err := loadSourceStateRow(ctx, tx, userPK, replacement.PreviousSourceID, true)
	if err != nil {
		return err
	}
	newState, err := loadSourceStateRow(ctx, tx, userPK, replacement.NewSourceID, true)
	if err != nil {
		return err
	}

	if previousState != nil && previousState.State == sourceStateRetired {
		if previousState.ReplacedBySourceID != replacement.NewSourceID {
			return &SourceRetiredError{
				UserID:             actor.UserID,
				SourceID:           replacement.PreviousSourceID,
				ReplacedBySourceID: previousState.ReplacedBySourceID,
			}
		}
		if newState == nil {
			if err := reserveSourceState(ctx, tx, userPK, replacement.NewSourceID); err != nil {
				return err
			}
		} else if newState.State != sourceStateReserved && newState.State != sourceStateActive {
			return &SourceReplacementInvalidError{Message: fmt.Sprintf("replacement source %s is not available for rotated rebuild", replacement.NewSourceID)}
		}
	} else {
		if newState != nil {
			return &SourceReplacementInvalidError{Message: fmt.Sprintf("replacement source %s is already known for user %s", replacement.NewSourceID, actor.UserID)}
		}
		if err := reserveSourceState(ctx, tx, userPK, replacement.NewSourceID); err != nil {
			return err
		}
		if err := retireSourceState(ctx, tx, userPK, replacement.PreviousSourceID, replacement.NewSourceID, replacement.Reason); err != nil {
			return err
		}
	}

	return nil
}

func buildTableSnapshotQuery(info registeredTableRuntimeInfo) string {
	tableIdent := pgx.Identifier{info.schemaName, info.tableName}.Sanitize()
	keyCol := pgx.Identifier{info.syncKeyColumn}.Sanitize()
	ownerCol := pgx.Identifier{syncScopeColumnName}.Sanitize()
	return fmt.Sprintf(`
		SELECT CAST(src.%s AS text), to_jsonb(src) - '_sync_scope_id'
		FROM %s AS src
		WHERE src.%s = $1
		ORDER BY CAST(src.%s AS text)
	`, keyCol, tableIdent, ownerCol, keyCol)
}

func readBatchTableRows(
	br pgx.BatchResults,
	info *registeredTableRuntimeInfo,
	liveRowState map[string]snapshotLiveState,
	seen map[string]struct{},
	rows *[]snapshotMaterializedRow,
	byteCount *int64,
	s *SyncService,
) error {
	liveRows, err := br.Query()
	if err != nil {
		return err
	}
	defer liveRows.Close()

	for liveRows.Next() {
		var (
			keyText   string
			payloadDB []byte
		)
		if err := liveRows.Scan(&keyText, &payloadDB); err != nil {
			return fmt.Errorf("scan live snapshot row for %s.%s: %w", info.schemaName, info.tableName, err)
		}
		keyBytes, _, err := encodeKeyBytes(info.syncKeyType, keyText)
		if err != nil {
			return fmt.Errorf("encode live snapshot key for %s.%s: %w", info.schemaName, info.tableName, err)
		}
		logicalKey := snapshotLogicalRowKey(info.tableID, keyBytes)
		state, ok := liveRowState[logicalKey]
		if !ok {
			return fmt.Errorf("live row %s.%s %q is missing non-deleted sync.row_state", info.schemaName, info.tableName, keyText)
		}
		if _, duplicate := seen[logicalKey]; duplicate {
			return fmt.Errorf("duplicate live snapshot row detected for %s.%s %q", info.schemaName, info.tableName, keyText)
		}
		seen[logicalKey] = struct{}{}

		payloadWire, err := s.canonicalizeWirePayload(info.schemaName, info.tableName, payloadDB)
		if err != nil {
			return fmt.Errorf("canonicalize live snapshot payload for %s.%s: %w", info.schemaName, info.tableName, err)
		}
		*byteCount += int64(len(payloadWire))
		*rows = append(*rows, snapshotMaterializedRow{
			tableID:     info.tableID,
			keyBytes:    append([]byte(nil), keyBytes...),
			bundleSeq:   state.bundleSeq,
			payloadWire: append([]byte(nil), payloadWire...),
		})
	}
	if err := liveRows.Err(); err != nil {
		return fmt.Errorf("iterate live snapshot rows for %s.%s: %w", info.schemaName, info.tableName, err)
	}
	return nil
}

func sortedTableInfos(registeredByID map[int32]registeredTableRuntimeInfo) []registeredTableRuntimeInfo {
	infos := make([]registeredTableRuntimeInfo, 0, len(registeredByID))
	for _, info := range registeredByID {
		infos = append(infos, info)
	}
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].tableID < infos[j].tableID
	})
	return infos
}

func (s *SyncService) GetSnapshotChunk(ctx context.Context, actor Actor, snapshotID string, afterRowOrdinal int64, maxRows int) (_ *SnapshotChunkResponse, err error) {
	done, err := s.beginOperation()
	if err != nil {
		return nil, err
	}
	defer done()
	if err := actor.validate(false); err != nil {
		return nil, err
	}
	if snapshotID == "" {
		return nil, &SnapshotChunkInvalidError{Message: "snapshot_id must be provided"}
	}
	if afterRowOrdinal < 0 {
		return nil, &SnapshotChunkInvalidError{Message: "after_row_ordinal must be >= 0"}
	}
	if maxRows <= 0 {
		return nil, &SnapshotChunkInvalidError{Message: "max_rows must be > 0"}
	}

	effectiveMaxRows := maxRows
	if effectiveMaxRows > s.maxRowsPerSnapshotChunk() {
		effectiveMaxRows = s.maxRowsPerSnapshotChunk()
	}

	var resp *SnapshotChunkResponse
	err = pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead}, func(tx pgx.Tx) error {
		var (
			sessionUserID     string
			snapshotBundleSeq int64
			expiresAt         time.Time
		)
		if err := tx.QueryRow(ctx, `
			SELECT us.user_id, ss.snapshot_bundle_seq, ss.expires_at
			FROM sync.snapshot_sessions AS ss
			JOIN sync.user_state AS us ON us.user_pk = ss.user_pk
			WHERE ss.snapshot_id = $1::uuid
		`, snapshotID).Scan(&sessionUserID, &snapshotBundleSeq, &expiresAt); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return &SnapshotSessionNotFoundError{SnapshotID: snapshotID}
			}
			return fmt.Errorf("query snapshot session: %w", err)
		}
		if sessionUserID != actor.UserID {
			return &SnapshotSessionForbiddenError{SnapshotID: snapshotID}
		}
		if time.Now().UTC().After(expiresAt) {
			return &SnapshotSessionExpiredError{SnapshotID: snapshotID}
		}

		queryRows, err := tx.Query(ctx, `
			SELECT row_ordinal, table_id, key_bytes, bundle_seq, payload_wire
			FROM sync.snapshot_session_rows
			WHERE snapshot_id = $1::uuid
			  AND row_ordinal > $2
			ORDER BY row_ordinal
			LIMIT $3
		`, snapshotID, afterRowOrdinal, effectiveMaxRows+1)
		if err != nil {
			return fmt.Errorf("query snapshot chunk rows: %w", err)
		}
		defer queryRows.Close()

		chunkRows := make([]SnapshotRow, 0, effectiveMaxRows+1)
		for queryRows.Next() {
			var (
				row        SnapshotRow
				tableID    int32
				keyBytes   []byte
				rowOrdinal int64
			)
			if err := queryRows.Scan(&rowOrdinal, &tableID, &keyBytes, &row.RowVersion, &row.Payload); err != nil {
				return fmt.Errorf("scan snapshot chunk row: %w", err)
			}
			info, err := s.tableInfoForID(tableID)
			if err != nil {
				return err
			}
			row.Schema = info.schemaName
			row.Table = info.tableName
			row.Key, err = wireSyncKeyFromBytes(info, keyBytes)
			if err != nil {
				return fmt.Errorf("decode snapshot chunk row key: %w", err)
			}
			chunkRows = append(chunkRows, row)
		}
		if err := queryRows.Err(); err != nil {
			return fmt.Errorf("iterate snapshot chunk rows: %w", err)
		}

		hasMore := false
		if len(chunkRows) > effectiveMaxRows {
			hasMore = true
			chunkRows = chunkRows[:effectiveMaxRows]
		}
		nextRowOrdinal := afterRowOrdinal
		if len(chunkRows) > 0 {
			nextRowOrdinal = afterRowOrdinal + int64(len(chunkRows))
		}

		resp = &SnapshotChunkResponse{
			SnapshotID:        snapshotID,
			SnapshotBundleSeq: snapshotBundleSeq,
			Rows:              chunkRows,
			NextRowOrdinal:    nextRowOrdinal,
			HasMore:           hasMore,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *SyncService) DeleteSnapshotSession(ctx context.Context, actor Actor, snapshotID string) (err error) {
	done, err := s.beginOperation()
	if err != nil {
		return err
	}
	defer done()
	if err := actor.validate(false); err != nil {
		return err
	}
	if snapshotID == "" {
		return &SnapshotChunkInvalidError{Message: "snapshot_id must be provided"}
	}

	return pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		var sessionUserID string
		if err := tx.QueryRow(ctx, `
			SELECT us.user_id
			FROM sync.snapshot_sessions AS ss
			JOIN sync.user_state AS us ON us.user_pk = ss.user_pk
			WHERE ss.snapshot_id = $1::uuid
		`, snapshotID).Scan(&sessionUserID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return &SnapshotSessionNotFoundError{SnapshotID: snapshotID}
			}
			return fmt.Errorf("query snapshot session for delete: %w", err)
		}
		if sessionUserID != actor.UserID {
			return &SnapshotSessionForbiddenError{SnapshotID: snapshotID}
		}
		if _, err := tx.Exec(ctx, `DELETE FROM sync.snapshot_sessions WHERE snapshot_id = $1::uuid`, snapshotID); err != nil {
			return fmt.Errorf("delete snapshot session: %w", err)
		}
		return nil
	})
}

func cleanupExpiredSnapshotSessionsQuerier(ctx context.Context, q interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}) error {
	if _, err := q.Exec(ctx, `DELETE FROM sync.snapshot_sessions WHERE expires_at <= now()`); err != nil {
		return fmt.Errorf("cleanup expired snapshot sessions: %w", err)
	}
	return nil
}
