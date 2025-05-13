package videostore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	// Blank importing SQLite3 driver for its side-effects.
	// This registers the "sqlite3" driver with the database/sql package.
	_ "github.com/mattn/go-sqlite3"
	"go.viam.com/rdk/logging"
	"go.viam.com/rdk/utils"
)

const (
	dbFileName        = "index.sqlite.db"
	pollingInterval   = 5 * time.Second
	segmentsTableName = "segments"
	dbFileMode        = 0o750
)

// Indexer manages metadata for video segments stored on disk.
type Indexer struct {
	logger      logging.Logger
	storagePath string
	dbPath      string
	db          *sql.DB
	setupDone   bool
}

// SegmentMetadata holds metadata for an indexed segment.
type SegmentMetadata struct {
	FilePath  string
	SizeBytes int64
}

// NewIndexer creates a new Indexer instance.
func NewIndexer(storagePath string, logger logging.Logger) *Indexer {
	dbPath := filepath.Join(storagePath, dbFileName)
	return &Indexer{
		logger:      logger,
		storagePath: storagePath,
		dbPath:      dbPath,
	}
}

// Setup performs IO and DB initialization. Must be called before use.
func (ix *Indexer) Setup() error {
	if ix.setupDone {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(ix.dbPath), dbFileMode); err != nil {
		return fmt.Errorf("failed to create directory for index db: %w", err)
	}
	db, err := sql.Open("sqlite3", ix.dbPath)
	if err != nil {
		return fmt.Errorf("failed to open index db: %w", err)
	}
	ix.db = db
	if err := ix.initializeDB(); err != nil {
		_ = ix.db.Close()
		ix.db = nil
		return fmt.Errorf("failed to initialize index db schema: %w", err)
	}
	ix.setupDone = true
	return nil
}

// initializeDB creates the necessary table if it doesn't exist.
func (ix *Indexer) initializeDB() error {
	query := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		file_path TEXT PRIMARY KEY,
		start_time_unix INTEGER NOT NULL,
		duration_ms INTEGER,
		size_bytes INTEGER,
		creation_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_start_time ON %s (start_time_unix);
	`, segmentsTableName, segmentsTableName)

	_, err := ix.db.Exec(query)
	return err
}

// Run starts the background polling loop for the indexer.
func (ix *Indexer) Run(ctx context.Context) {
	ix.logger.Debug("starting Indexer polling loop")
	ticker := time.NewTicker(pollingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			ix.logger.Debug("indexer polling loop has stopped")
			return
		case <-ticker.C:
			if err := ix.scanAndIndex(); err != nil {
				ix.logger.Errorw("error during indexer scan", "error", err)
			}
		}
	}
}

// scanAndIndex performs one cycle of scanning the storage directory and updating the index.
// It focuses on adding new files found on disk to the index.
// It does not remove index entries for deleted disk files; that is handled by the cleanup/deletion logic.
func (ix *Indexer) scanAndIndex() error {
	if !ix.setupDone {
		return errors.New("indexer setup not complete")
	}
	// Get disk files
	diskFiles, err := ix.getDiskFiles()
	if err != nil {
		ix.logger.Warnw("failed to list disk files, skipping scan cycle", "path", ix.storagePath, "error", err)
		return nil
	}

	// Get indexed files
	indexedFiles, err := ix.getIndexedFiles()
	if err != nil {
		return fmt.Errorf("failed to get indexed files from DB: %w", err)
	}

	// Diff disk files and indexed files
	for path, info := range diskFiles {
		if _, exists := indexedFiles[path]; !exists {
			ix.logger.Debugf("found new segment to index: %s", path)
			if err := ix.indexNewFile(path, info); err != nil {
				ix.logger.Errorw("failed to index new file", "path", path, "error", err)
			}
		}
	}

	return nil
}

// getIndexedFiles retrieves the set of file paths currently in the index.
func (ix *Indexer) getIndexedFiles() (map[string]struct{}, error) {
	if !ix.setupDone {
		return nil, errors.New("indexer setup not complete")
	}
	query := "SELECT file_path FROM " + segmentsTableName + ";"
	rows, err := ix.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	files := make(map[string]struct{})
	for rows.Next() {
		var filePath string
		if err := rows.Scan(&filePath); err != nil {
			return nil, err
		}
		files[filePath] = struct{}{}
	}
	return files, rows.Err()
}

// getDiskFiles retrieves the set of .mp4 files currently in the storage directory.
// It interacts with the filesystem only and does not require the indexer mutex.
func (ix *Indexer) getDiskFiles() (map[string]os.FileInfo, error) {
	files := make(map[string]os.FileInfo)
	entries, err := os.ReadDir(ix.storagePath)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".mp4") {
			info, err := entry.Info()
			if err != nil {
				ix.logger.Warnw("failed to get FileInfo for disk file", "name", entry.Name(), "error", err)
				continue
			}
			fullPath := filepath.Join(ix.storagePath, entry.Name())
			files[fullPath] = info
		}
	}
	return files, nil
}

// indexNewFile extracts metadata and adds a new file to the index.
func (ix *Indexer) indexNewFile(filePath string, fileInfo os.FileInfo) error {
	startTime, err := extractDateTimeFromFilename(filePath)
	if err != nil {
		ix.logger.Warnw("failed to extract timestamp from filename, skipping", "file", filePath, "error", err)
		return nil
	}

	durationMs, err := ix.getVideoDurationMs(filePath)
	if err != nil {
		ix.logger.Debugf("failed to get video info, unreadable file will not be indexed: %w", err)
		return nil
	}

	sizeBytes := fileInfo.Size()

	query := fmt.Sprintf("INSERT INTO %s (file_path, start_time_unix, duration_ms, size_bytes) VALUES (?, ?, ?, ?)", segmentsTableName)
	_, err = ix.db.Exec(query, filePath, startTime.Unix(), durationMs, sizeBytes)
	if err != nil {
		return fmt.Errorf("failed to insert segment into index: %w", err)
	}

	startTimeStr := startTime.UTC().Format(TimeFormat) + "Z"
	ix.logger.Debugw("indexed new file", "file", filePath, "start_time", startTimeStr, "duration_ms", durationMs, "size_bytes", sizeBytes)
	return nil
}

// RemoveIndexedFiles removes file paths from the index.
func (ix *Indexer) RemoveIndexedFiles(filePaths []string) error {
	if !ix.setupDone {
		return errors.New("indexer setup not complete")
	}
	if len(filePaths) == 0 {
		return nil
	}

	tx, err := ix.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	g := utils.NewGuard(func() {
		err := tx.Rollback()
		if err != nil {
			ix.logger.Errorw("failed to rollback transaction", "error", err)
		}
	})
	defer g.Success()

	query := fmt.Sprintf("DELETE FROM %s WHERE file_path = ?;", segmentsTableName)
	stmt, err := tx.Prepare(query)
	if err != nil {
		return fmt.Errorf("failed to prepare delete statement: %w", err)
	}
	defer stmt.Close()

	for _, path := range filePaths {
		if _, err := stmt.Exec(path); err != nil {
			return fmt.Errorf("failed to execute delete for path '%s': %w", path, err)
		}
		ix.logger.Debugf("removed indexed segment: %s", path)
	}

	return tx.Commit()
}

// getVideoDurationMs uses the getVideoInfo utility to get the duration of a video file.
// This function does not require the indexer mutex.
func (ix *Indexer) getVideoDurationMs(filePath string) (int64, error) {
	info, err := getVideoInfo(filePath)
	if err != nil {
		return 0, fmt.Errorf("getVideoInfo failed for %s: %w", filePath, err)
	}
	// getVideoInfo returns duration in microseconds, convert to milliseconds
	durationMs := info.duration.Milliseconds()
	return durationMs, nil
}

// timeRange represents a single contiguous block of stored video.
type timeRange struct {
	Start time.Time
	End   time.Time
}

// StorageState summarizes the state of the stored video segments.
type StorageState struct {
	TotalSizeBytes  int64
	TotalDurationMs int64
	SegmentCount    int
	Ranges          []timeRange // List of contiguous time ranges
}

// GetStorageState calculates the current storage state from the index.
func (ix *Indexer) GetStorageState() (StorageState, error) {
	if !ix.setupDone {
		return StorageState{}, errors.New("indexer setup not complete")
	}
	var state StorageState

	// Query all segments, ordered by start time
	query := fmt.Sprintf(`
	SELECT start_time_unix, duration_ms, size_bytes
	FROM %s
	ORDER BY start_time_unix ASC
	`, segmentsTableName)

	rows, err := ix.db.Query(query)
	if err != nil {
		return StorageState{}, fmt.Errorf("failed to query segments for state: %w", err)
	}
	defer rows.Close()

	var ranges []timeRange
	var currentRange *timeRange
	const slopDuration = 5 * time.Second

	for rows.Next() {
		var startTimeUnix int64
		var durationMs sql.NullInt64
		var sizeBytes sql.NullInt64

		if err := rows.Scan(&startTimeUnix, &durationMs, &sizeBytes); err != nil {
			return StorageState{}, fmt.Errorf("failed to scan segment row: %w", err)
		}

		state.SegmentCount++
		if durationMs.Valid {
			state.TotalDurationMs += durationMs.Int64
		} else {
			ix.logger.Warnw("segment found with NULL duration in index", "start_time_unix", startTimeUnix)
			continue
		}
		if sizeBytes.Valid {
			state.TotalSizeBytes += sizeBytes.Int64
		}

		segmentStart := time.Unix(startTimeUnix, 0)
		segmentEnd := segmentStart.Add(time.Duration(durationMs.Int64) * time.Millisecond)

		if currentRange == nil {
			currentRange = &timeRange{Start: segmentStart, End: segmentEnd}
		} else {
			if segmentStart.After(currentRange.End.Add(slopDuration)) {
				ranges = append(ranges, *currentRange)
				currentRange = &timeRange{Start: segmentStart, End: segmentEnd}
			} else {
				currentRange.End = segmentEnd
			}
		}
	}
	if currentRange != nil {
		ranges = append(ranges, *currentRange)
	}

	if err := rows.Err(); err != nil {
		return StorageState{}, fmt.Errorf("error iterating over all segments query result: %w", err)
	}

	state.Ranges = ranges
	return state, nil
}

// GetSegmentsAscTime retrieves all indexed segments in ascending order by start time.
// Returns a slice of SegmentMetadata containing file path and size.
func (ix *Indexer) GetSegmentsAscTime() ([]SegmentMetadata, error) {
	if !ix.setupDone {
		return nil, errors.New("indexer setup not complete")
	}
	query := fmt.Sprintf(`
	SELECT file_path, size_bytes
	FROM %s
	ORDER BY start_time_unix ASC
	`, segmentsTableName)

	rows, err := ix.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query all segments: %w", err)
	}
	defer rows.Close()

	var segments []SegmentMetadata

	for rows.Next() {
		var filePath string
		var sizeBytes sql.NullInt64

		if err := rows.Scan(&filePath, &sizeBytes); err != nil {
			ix.logger.Errorw("failed to scan segment row during full query", "error", err)
			continue
		}

		if sizeBytes.Valid {
			segments = append(segments, SegmentMetadata{
				FilePath:  filePath,
				SizeBytes: sizeBytes.Int64,
			})
		} else {
			ix.logger.Warnw("segment found with NULL size in index, skipping", "file_path", filePath)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over all segments query result: %w", err)
	}

	ix.logger.Debugf("retrieved %d segments from index", len(segments))
	return segments, nil
}

// Close shuts down the indexer and closes the database connection.
func (ix *Indexer) Close() error {
	if ix.db != nil {
		err := ix.db.Close()
		if err != nil {
			ix.logger.Errorw("error closing index db", "error", err)
		}
		ix.db = nil
		ix.logger.Debug("indexer closed")
	}
	ix.setupDone = false
	return nil
}
