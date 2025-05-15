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
)

const (
	dbFileName             = "index.sqlite.db"
	dbFileMode             = 0o750
	segmentsTableName      = "segments"
	videoFileSuffix        = ".mp4"
	indexerPollingInterval = 10 * time.Second
	slopDuration           = 5 * time.Second
)

// indexer manages metadata for video segments stored on disk.
type indexer struct {
	logger       logging.Logger
	storagePath  string
	storageMaxGB int
	dbPath       string
	db           *sql.DB
	setupDone    bool
}

// segmentMetadata holds metadata for an indexed segment.
type segmentMetadata struct {
	FilePath      string
	StartTimeUnix int64
	DurationMs    int64
	SizeBytes     int64
}

// newIndexer creates a new indexer instance.
func newIndexer(storagePath string, storageMaxGB int, logger logging.Logger) *indexer {
	dbPath := filepath.Join(storagePath, dbFileName)
	return &indexer{
		logger:       logger,
		storagePath:  storagePath,
		storageMaxGB: storageMaxGB,
		dbPath:       dbPath,
	}
}

func (ix *indexer) setup() error {
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
		return fmt.Errorf("failed to initialize index db schema: %w", err)
	}
	ix.setupDone = true
	return nil
}

func (ix *indexer) initializeDB() error {
	query := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		file_path TEXT PRIMARY KEY,
		start_time_unix INTEGER NOT NULL,
		duration_ms INTEGER NOT NULL,
		size_bytes INTEGER NOT NULL,
		inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_start_time ON %s (start_time_unix);
	`, segmentsTableName, segmentsTableName)

	_, err := ix.db.Exec(query)
	return err
}

func (ix *indexer) run(ctx context.Context) {
	ix.logger.Debug("starting Indexer polling loop")
	ticker := time.NewTicker(indexerPollingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			ix.logger.Debug("indexer polling loop has stopped")
			return
		case <-ticker.C:
			if err := ix.cleanupAndIndex(ctx); err != nil {
				ix.logger.Errorw("error during indexer maintenance", "error", err)
			}
		}
	}
}

func (ix *indexer) cleanupAndIndex(ctx context.Context) error {
	// 1. Clean up db according to storage limits
	deletedFilePaths, err := ix.cleanupDB(ctx)
	if err != nil {
		ix.logger.Errorw("failed to cleanup db", "error", err)
	}

	// 2. Delete disk files that were just deleted from db
	for _, path := range deletedFilePaths {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := os.Remove(path); err == nil {
			ix.logger.Infof("deleted file from disk: %s", path)
		} else {
			ix.logger.Warnw("failed to delete file from disk", "file", path, "error", err)
		}
	}

	// 3. Index new files
	if err := ix.indexNewFiles(ctx); err != nil {
		return err
	}
	return nil
}

// indexNewFiles indexes new video files on disk.
func (ix *indexer) indexNewFiles(ctx context.Context) error {
	diskFiles, err := ix.getDiskFiles(ctx)
	if err != nil {
		return err
	}
	indexedFiles, err := ix.getIndexedFiles(ctx)
	if err != nil {
		return err
	}

	// Query for the oldest DB file in order to determine if a file is new
	var oldestDBFile string
	row := ix.db.QueryRowContext(ctx, fmt.Sprintf("SELECT file_path FROM %s ORDER BY start_time_unix ASC LIMIT 1", segmentsTableName))
	if err := row.Scan(&oldestDBFile); err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("failed to get oldest DB entry: %w", err)
	}
	var oldestDBTime time.Time
	if oldestDBFile != "" {
		oldestDBTime, err = extractDateTimeFromFilename(oldestDBFile)
		if err != nil {
			return fmt.Errorf("failed to parse filename of oldest db entry %q: %w", oldestDBFile, err)
		}
	}

	for path, info := range diskFiles {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if _, exists := indexedFiles[path]; exists {
			continue
		}
		if !oldestDBTime.IsZero() {
			fileTime, err := extractDateTimeFromFilename(path)
			if err != nil {
				return fmt.Errorf("failed to extract timestamp from filename, skipping: %w", err)
			}
			if fileTime.Before(oldestDBTime) {
				continue
			}
		}
		if err := ix.indexNewFile(path, info.Size()); err != nil {
			ix.logger.Errorw("failed to index new file", "path", path, "error", err)
		}
	}
	return nil
}

// indexNewFile is a helper function that indexes a new video file in the db.
func (ix *indexer) indexNewFile(filePath string, fileSize int64) error {
	startTime, err := extractDateTimeFromFilename(filePath)
	if err != nil {
		ix.logger.Warnw("failed to extract timestamp from filename, skipping", "file", filePath, "error", err)
		return nil
	}

	info, err := getVideoInfo(filePath)
	if err != nil {
		ix.logger.Debugf("failed to get video info, unreadable file will not be indexed: %w", err)
		return nil
	}
	durationMs := info.duration.Milliseconds()

	query := fmt.Sprintf("INSERT INTO %s (file_path, start_time_unix, duration_ms, size_bytes) VALUES (?, ?, ?, ?);", segmentsTableName)
	_, err = ix.db.Exec(query, filePath, startTime.Unix(), durationMs, fileSize)
	if err != nil {
		return fmt.Errorf("failed to insert segment into index: %w", err)
	}

	startTimeStr := FormatDateTimeString(startTime)
	ix.logger.Debugw("indexed new file", "file", filePath, "start_time", startTimeStr, "duration_ms", durationMs, "size_bytes", fileSize)
	return nil
}

// cleanupDB deletes old records and returns the paths of the to-be-deleted segment files.
func (ix *indexer) cleanupDB(ctx context.Context) ([]string, error) {
	maxStorageSizeBytes := int64(ix.storageMaxGB) * gigabyte
	currentSizeBytes, err := getDirectorySize(ix.storagePath)
	if err != nil {
		return nil, err
	}
	if currentSizeBytes < maxStorageSizeBytes {
		return nil, nil
	}
	bytesToDelete := currentSizeBytes - maxStorageSizeBytes

	allSegments, err := ix.getSegmentsAscTime(ctx)
	if err != nil {
		return nil, err
	}
	if len(allSegments) == 0 {
		return nil, errors.New("cleanup required based on disk size, but indexer found no segments")
	}

	var (
		pathsToDelete []string
		bytesDeleted  int64
	)
	for _, segment := range allSegments {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		pathsToDelete = append(pathsToDelete, segment.FilePath)
		bytesDeleted += segment.SizeBytes
		if bytesDeleted >= bytesToDelete {
			break
		}
	}
	if len(pathsToDelete) == 0 {
		return nil, nil
	}
	deletedFilePaths, err := ix.deleteSegmentsAndReturnPaths(ctx, pathsToDelete)
	if err != nil {
		return nil, err
	}
	ix.logger.Infof("deleted %d segments from db (soon to be deleted from disk): %v", len(deletedFilePaths), deletedFilePaths)
	return deletedFilePaths, nil
}

// deleteSegmentsAndReturnPaths is a helper function that deletes segments from the DB
// and returns the paths of the deleted segments.
func (ix *indexer) deleteSegmentsAndReturnPaths(ctx context.Context, paths []string) ([]string, error) {
	if !ix.setupDone {
		return nil, errors.New("indexer setup not complete")
	}
	if len(paths) == 0 {
		return nil, nil
	}

	// Build the SQL query
	placeholders := make([]string, len(paths))
	args := make([]interface{}, len(paths))
	for i, path := range paths {
		placeholders[i] = "?"
		args[i] = path
	}
	//nolint:gosec // this is safe because segmentsTableName is a constant and placeholders are '?'
	query := fmt.Sprintf("DELETE FROM %s WHERE file_path IN (%s) RETURNING file_path;", segmentsTableName, strings.Join(placeholders, ", "))

	// QueryContext is used because the DELETE statement includes a RETURNING clause to get the deleted file paths.
	rows, err := ix.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var deletedPaths []string
	for rows.Next() {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		var filePath string
		if err := rows.Scan(&filePath); err != nil {
			return nil, err
		}
		deletedPaths = append(deletedPaths, filePath)
	}
	return deletedPaths, rows.Err()
}

// getIndexedFiles returns a map of all indexed video files from the db.
func (ix *indexer) getIndexedFiles(ctx context.Context) (map[string]struct{}, error) {
	if !ix.setupDone {
		return nil, errors.New("indexer setup not complete")
	}
	query := "SELECT file_path FROM " + segmentsTableName + ";"
	rows, err := ix.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	files := make(map[string]struct{})
	for rows.Next() {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		var filePath string
		if err := rows.Scan(&filePath); err != nil {
			return nil, err
		}
		files[filePath] = struct{}{}
	}
	return files, rows.Err()
}

// getDiskFiles returns a map of all video files on disk.
func (ix *indexer) getDiskFiles(ctx context.Context) (map[string]os.FileInfo, error) {
	files := make(map[string]os.FileInfo)
	entries, err := os.ReadDir(ix.storagePath)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), videoFileSuffix) {
			info, err := entry.Info()
			if err != nil {
				return nil, fmt.Errorf("failed to get FileInfo for disk file: %s, error: %w", entry.Name(), err)
			}
			fullPath := filepath.Join(ix.storagePath, entry.Name())
			files[fullPath] = info
		}
	}
	return files, nil
}

// videoRange represents a single contiguous block of stored video segments.
type videoRange struct {
	Start time.Time
	End   time.Time
}

// videoRanges summarizes the state of the stored video segments.
type videoRanges struct {
	TotalSizeBytes  int64
	TotalDurationMs int64
	VideoCount      int
	Ranges          []videoRange
}

// getVideoList returns the full list of video range structs from the db.
func (ix *indexer) getVideoList(ctx context.Context) (videoRanges, error) {
	segments, err := ix.getSegmentsAscTime(ctx)
	if err != nil {
		return videoRanges{}, fmt.Errorf("failed to fetch segments for state: %w", err)
	}

	var videoRanges videoRanges
	if len(segments) == 0 {
		return videoRanges, nil
	}

	var prevRange *videoRange

	for _, s := range segments {
		videoRanges.VideoCount++
		videoRanges.TotalDurationMs += s.DurationMs
		videoRanges.TotalSizeBytes += s.SizeBytes

		segmentStart := time.Unix(s.StartTimeUnix, 0)
		segmentEnd := segmentStart.Add(time.Duration(s.DurationMs) * time.Millisecond)

		if prevRange == nil {
			prevRange = &videoRange{Start: segmentStart, End: segmentEnd}
		} else {
			if segmentStart.After(prevRange.End.Add(slopDuration)) {
				videoRanges.Ranges = append(videoRanges.Ranges, *prevRange)
				prevRange = &videoRange{Start: segmentStart, End: segmentEnd}
			} else {
				prevRange.End = segmentEnd
			}
		}
	}
	if prevRange != nil {
		videoRanges.Ranges = append(videoRanges.Ranges, *prevRange)
	}

	return videoRanges, nil
}

// getSegmentsAscTime is a helper function that retrieves all segment data from the database, ordered by start time.
func (ix *indexer) getSegmentsAscTime(ctx context.Context) ([]segmentMetadata, error) {
	if !ix.setupDone {
		return nil, errors.New("indexer setup not complete")
	}
	query := fmt.Sprintf(`
	SELECT file_path, start_time_unix, duration_ms, size_bytes
	FROM %s
	ORDER BY start_time_unix ASC
	`, segmentsTableName)

	rows, err := ix.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query all segments: %w", err)
	}
	defer rows.Close()

	var segments []segmentMetadata

	for rows.Next() {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		var sm segmentMetadata

		if err := rows.Scan(&sm.FilePath, &sm.StartTimeUnix, &sm.DurationMs, &sm.SizeBytes); err != nil {
			return nil, fmt.Errorf("failed to scan segment row during full query: %w", err)
		}

		segments = append(segments, sm)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over all segments query result: %w", err)
	}

	ix.logger.Debugf("retrieved %d segments from index", len(segments))
	return segments, nil
}

func (ix *indexer) close() error {
	if ix.setupDone {
		err := ix.db.Close()
		if err != nil {
			return fmt.Errorf("error closing index db: %w", err)
		}
		ix.setupDone = false
		ix.logger.Debug("indexer closed")
	}
	return nil
}
