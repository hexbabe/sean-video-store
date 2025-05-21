// Package indexer manages metadata for video files stored on disk.
package indexer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mattn/go-sqlite3"
	vsutils "github.com/viam-modules/video-store/videostore/utils"
	"go.viam.com/rdk/logging"
)

const (
	dbFileName        = "index.sqlite.db"
	dbFileMode        = 0o750
	segmentsTableName = "segments"
	videoFileSuffix   = ".mp4"
	refreshalInterval = 1 * time.Second
	slopDuration      = 5 * time.Second
)

// diskFileEntry holds information about a file on disk being considered for indexing,
// including its extracted timestamp.
type diskFileEntry struct {
	info os.FileInfo
	time time.Time // extracted from the filename
}

// Indexer manages metadata for video segments stored on disk.
type Indexer struct {
	logger       logging.Logger
	storagePath  string
	storageMaxGB int
	dbPath       string
	db           *sql.DB
	dbPtrMutex   sync.RWMutex
}

// segmentMetadata holds metadata for an indexed segment.
type segmentMetadata struct {
	FileName      string
	StartTimeUnix int64
	DurationMs    int64
	SizeBytes     int64
	Width         int
	Height        int
	Codec         string
}

// NewIndexer creates a new indexer instance.
func NewIndexer(storagePath string, storageMaxGB int, logger logging.Logger) *Indexer {
	dbPath := filepath.Join(storagePath, dbFileName)
	idx := &Indexer{
		logger:       logger,
		storagePath:  storagePath,
		storageMaxGB: storageMaxGB,
		dbPath:       dbPath,
	}
	return idx
}

// setupDB initializes the underlying database and readies it for use.
func (ix *Indexer) setupDB(ctx context.Context) error {
	ix.dbPtrMutex.Lock()
	defer ix.dbPtrMutex.Unlock()

	if ix.db != nil {
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
	if err := ix.initializeDB(ctx); err != nil {
		_ = ix.db.Close()
		return fmt.Errorf("failed to initialize index db schema: %w", err)
	}
	return nil
}

// initializeDB initializes the database schema. Assumes the dbPtrMutex is write-locked.
func (ix *Indexer) initializeDB(ctx context.Context) error {
	query := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		id INTEGER NOT NULL PRIMARY KEY,
		file_name TEXT NOT NULL UNIQUE,
		start_time_unix INTEGER NOT NULL,
		duration_ms INTEGER NOT NULL,
		size_bytes INTEGER NOT NULL,
		width INTEGER NOT NULL,
		height INTEGER NOT NULL,
		codec TEXT NOT NULL,
		inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
		deleted_at TIMESTAMP
	);
	`, segmentsTableName)

	_, err := ix.db.ExecContext(ctx, query)
	return err
}

// Run starts the indexer event loop.
func (ix *Indexer) Run(ctx context.Context) {
	ix.logger.Debug("starting indexer event loop")
	ticker := time.NewTicker(refreshalInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			ix.logger.Debug("indexer event loop has stopped")
			return
		case <-ticker.C:
			err := ix.setupDB(ctx)
			if err != nil {
				ix.logger.Errorw("error setting up indexer", "error", err)
				continue
			}
			err = ix.refreshIndexAndStorage(ctx)
			if err != nil {
				if isDBReadOnlyError(err) {
					ix.logger.Warnw("readonly DB error detected by indexer refresh, attempting re-setup.", "error", err)
					err = ix.Close()
					if err != nil {
						ix.logger.Errorw("error closing readonly db", "error", err)
					}
				} else {
					ix.logger.Errorw("error refreshing indexer", "error", err)
				}
			}
		}
	}
}

func isDBReadOnlyError(err error) bool {
	var sErr sqlite3.Error
	return errors.As(err, &sErr) && sErr.Code == sqlite3.ErrReadonly
}

// refreshIndexAndStorage refreshes the index and storage.
func (ix *Indexer) refreshIndexAndStorage(ctx context.Context) error {
	ix.dbPtrMutex.RLock()
	defer ix.dbPtrMutex.RUnlock()
	if ix.db == nil {
		return errors.New("indexer not setup")
	}

	start := time.Now()

	// 1. Index new files
	indexStart := time.Now()
	if err := ix.indexNewFiles(ctx); err != nil {
		return err
	}
	ix.logger.Debugf("TIMING: indexNewFiles took %v", time.Since(indexStart))

	// 2. Mark files to delete based on storage limits
	cleanupDBStart := time.Now()
	if err := ix.cleanupDB(ctx); err != nil {
		return err
	}
	ix.logger.Debugf("TIMING: cleanupDB took %v", time.Since(cleanupDBStart))

	// 3. Delete marked files from disk and their records from DB
	cleanupFilesStart := time.Now()
	if err := ix.cleanupFiles(ctx); err != nil {
		return err
	}
	ix.logger.Debugf("TIMING: cleanupFiles took %v", time.Since(cleanupFilesStart))

	ix.logger.Debugf("TIMING: refreshIndexAndStorage completed in %v", time.Since(start))
	return nil
}

// indexNewFiles indexes new video files on disk. Assumes the dbPtrMutex is read-locked.
func (ix *Indexer) indexNewFiles(ctx context.Context) error {
	indexedFiles, err := ix.getIndexedFiles(ctx)
	if err != nil {
		return fmt.Errorf("failed to get indexed files: %w", err)
	}

	diskFileEntries, err := ix.getDiskFilesSorted(ctx)
	if err != nil {
		return fmt.Errorf("failed to get and sort disk files: %w", err)
	}

	for _, entry := range diskFileEntries {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		fileName := entry.info.Name()
		if _, exists := indexedFiles[fileName]; exists {
			continue
		}

		err := ix.indexNewFile(ctx, fileName, entry.info.Size())
		if err != nil {
			if isDBReadOnlyError(err) {
				return err
			}
			ix.logger.Errorw("failed to index new file", "name", fileName, "error", err)
		}
	}

	if err := ix.handleMissingDiskFiles(ctx, indexedFiles); err != nil {
		return fmt.Errorf("failed to handle missing disk files: %w", err)
	}

	return nil
}

// getIndexedFiles returns a map of all non-deleted indexed video file names from the db.
// Assumes the dbPtrMutex is read-locked.
func (ix *Indexer) getIndexedFiles(ctx context.Context) (map[string]struct{}, error) {
	query := "SELECT file_name FROM " + segmentsTableName + " WHERE deleted_at IS NULL;"
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
		var fileName string
		if err := rows.Scan(&fileName); err != nil {
			return nil, err
		}
		files[fileName] = struct{}{}
	}
	return files, rows.Err()
}

// getDiskFilesSorted returns a slice of diskFileEntry for all valid video files on disk,
// sorted by their extracted timestamp (newest first).
// Files with unparseable names are logged and skipped.
func (ix *Indexer) getDiskFilesSorted(ctx context.Context) ([]diskFileEntry, error) {
	entries, err := os.ReadDir(ix.storagePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read storage directory %s: %w", ix.storagePath, err)
	}

	sortableFiles := make([]diskFileEntry, 0, len(entries))
	for _, entry := range entries {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if entry.IsDir() || !strings.HasSuffix(entry.Name(), videoFileSuffix) {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			ix.logger.Warnw("failed to get FileInfo for disk entry, skipping", "entry_name", entry.Name(), "error", err)
			continue
		}

		extractedTime, err := vsutils.ExtractDateTimeFromFilename(info.Name())
		if err != nil {
			ix.logger.Warnw("failed to extract timestamp from filename, skipping file", "name", info.Name(), "error", err)
			continue
		}

		sortableFiles = append(sortableFiles, diskFileEntry{info: info, time: extractedTime})
	}

	// Sort files by extracted time (newest first)
	sort.Slice(sortableFiles, func(i, j int) bool {
		return sortableFiles[i].time.After(sortableFiles[j].time)
	})
	return sortableFiles, nil
}

// indexNewFile is a helper function that indexes a new video file in the db. Assumes the dbPtrMutex is read-locked.
func (ix *Indexer) indexNewFile(ctx context.Context, fileName string, fileSize int64) error {
	startTime, err := vsutils.ExtractDateTimeFromFilename(fileName)
	if err != nil {
		ix.logger.Warnw("failed to extract timestamp from filename, skipping", "file", fileName, "error", err)
		return nil
	}

	fullFilePath := filepath.Join(ix.storagePath, fileName)
	info, err := vsutils.GetVideoInfo(fullFilePath)
	if err != nil {
		ix.logger.Debugw("failed to get video info, unreadable file will not be indexed", "file", fileName, "error", err)
		return nil
	}
	durationMs := info.Duration.Milliseconds()

	query := fmt.Sprintf(
		"INSERT OR IGNORE INTO %s (file_name, start_time_unix, duration_ms, size_bytes, width, height, codec) VALUES (?, ?, ?, ?, ?, ?, ?);",
		segmentsTableName,
	)
	_, err = ix.db.ExecContext(
		ctx,
		query,
		fileName,
		startTime.Unix(),
		durationMs,
		fileSize,
		info.Width,
		info.Height,
		info.Codec,
	)
	if err != nil {
		return fmt.Errorf("failed to insert segment into index: %w", err)
	}

	startTimeStr := vsutils.FormatUTC(startTime)
	ix.logger.Debugw("indexed new file", "file", fileName, "start_time", startTimeStr, "duration_ms", durationMs, "size_bytes", fileSize)
	return nil
}

// handleMissingDiskFiles checks for files that are indexed in the DB but no longer exist on disk.
// It marks such files as deleted in the database.
func (ix *Indexer) handleMissingDiskFiles(ctx context.Context, indexedFiles map[string]struct{}) error {
	var filesMissingFromDisk []string
	for indexedFileName := range indexedFiles {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fullPath := filepath.Join(ix.storagePath, indexedFileName)
		if _, statErr := os.Stat(fullPath); os.IsNotExist(statErr) {
			ix.logger.Debugw("indexed file not found on disk, marking for deletion", "file", indexedFileName)
			filesMissingFromDisk = append(filesMissingFromDisk, indexedFileName)
		}
	}

	if len(filesMissingFromDisk) > 0 {
		if err := ix.markSegmentsAsDeleted(ctx, filesMissingFromDisk); err != nil {
			return fmt.Errorf("failed to mark missing disk files as deleted in DB: %w", err)
		}
		ix.logger.Debugf("marked %d files missing from disk as deleted in DB", len(filesMissingFromDisk))
	}
	return nil
}

// cleanupDB determines which segment files should be deleted based on storage limits
// and marks them in the database by setting deleted_at. Assumes the dbPtrMutex is read-locked.
func (ix *Indexer) cleanupDB(ctx context.Context) error {
	maxStorageSizeBytes := int64(ix.storageMaxGB) * vsutils.Gigabyte
	currentSizeBytes, err := vsutils.GetDirectorySize(ix.storagePath)
	if err != nil {
		return fmt.Errorf("failed to get directory size for %s: %w", ix.storagePath, err)
	}
	if currentSizeBytes < maxStorageSizeBytes {
		return nil
	}
	bytesToDelete := currentSizeBytes - maxStorageSizeBytes

	segments, err := ix.getSegmentsAscTime(ctx)
	if err != nil {
		return fmt.Errorf("failed to get segments for cleanup: %w", err)
	}
	if len(segments) == 0 {
		return errors.New("no segments found in index, but cleanup required based on disk size")
	}

	var filesToMarkDeleted []string
	var bytesMarkedForDeletion int64
	for _, segment := range segments {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		filesToMarkDeleted = append(filesToMarkDeleted, segment.FileName)
		bytesMarkedForDeletion += segment.SizeBytes
		if bytesMarkedForDeletion >= bytesToDelete {
			break
		}
	}
	if len(filesToMarkDeleted) == 0 {
		return nil
	}

	err = ix.markSegmentsAsDeleted(ctx, filesToMarkDeleted)
	if err != nil {
		return fmt.Errorf("failed to mark segments as deleted in DB: %w", err)
	}

	return nil
}

// markSegmentsAsDeleted sets the deleted_at timestamp for the given file names. Assumes the dbPtrMutex is read-locked.
func (ix *Indexer) markSegmentsAsDeleted(ctx context.Context, names []string) error {
	if len(names) == 0 {
		return nil
	}

	// Build up query
	currentTime := time.Now().UTC()
	placeholders := make([]string, len(names))
	args := make([]interface{}, len(names)+1) // +1 for currentTime
	args[0] = currentTime
	for i, name := range names {
		placeholders[i] = "?"
		args[i+1] = name
	}
	//nolint:gosec // segmentsTableName is a constant, placeholders are '?'
	query := fmt.Sprintf(
		"UPDATE %s SET deleted_at = ? WHERE file_name IN (%s) AND deleted_at IS NULL;",
		segmentsTableName,
		strings.Join(placeholders, ", "),
	)

	result, err := ix.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to execute mark segments as deleted: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if int(rowsAffected) != len(names) {
		return fmt.Errorf("expected to mark %d segments as deleted, but only %d were affected", len(names), rowsAffected)
	}

	return nil
}

// cleanupFiles queries for segments marked with deleted_at,
// deletes their files from disk, and then removes their records from the database.
// Assumes the dbPtrMutex is read-locked.
func (ix *Indexer) cleanupFiles(ctx context.Context) error {
	ix.logger.Debug("starting cleanupFiles: querying for segments marked as deleted")

	query := fmt.Sprintf("SELECT file_name FROM %s WHERE deleted_at IS NOT NULL;", segmentsTableName)
	rows, err := ix.db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query for soft-deleted segments: %w", err)
	}
	defer rows.Close()

	// Scan db and accumulate files to be deleted (marked for deletion as per deleted_at)
	var filesToDelete []string
	for rows.Next() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var fileName string
		if err := rows.Scan(&fileName); err != nil {
			return fmt.Errorf("failed to scan soft-deleted segment path: %w", err)
		}
		filesToDelete = append(filesToDelete, fileName)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating over soft-deleted segment paths: %w", err)
	}

	if len(filesToDelete) == 0 {
		ix.logger.Debug("no soft-deleted segments found to process in cleanupFiles")
		return nil
	}

	for _, name := range filesToDelete {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Delete from disk
		fullFilePath := filepath.Join(ix.storagePath, name)
		fileErr := os.Remove(fullFilePath)
		if fileErr == nil {
			ix.logger.Debugf("deleted file from disk: %s", fullFilePath)
		} else if os.IsNotExist(fileErr) {
			ix.logger.Warnf("file already deleted from disk or never existed: %s", fullFilePath)
		} else {
			return fmt.Errorf("failed to delete file from disk: %w", fileErr)
		}

		// Hard delete from database
		deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE file_name = ?;", segmentsTableName)
		_, dbErr := ix.db.ExecContext(ctx, deleteQuery, name)
		if dbErr != nil {
			return fmt.Errorf("failed to hard-delete segment record from database: %w", dbErr)
		}
	}

	return nil
}

// VideoRange represents a single contiguous block of stored video segments.
type VideoRange struct {
	Start time.Time
	End   time.Time
}

// VideoRanges summarizes the state of the stored video segments.
type VideoRanges struct {
	StorageUsedBytes int64
	TotalDurationMs  int64
	VideoCount       int
	Ranges           []VideoRange
}

// GetVideoList returns the full list of video range structs from the db.
func (ix *Indexer) GetVideoList(ctx context.Context) (VideoRanges, error) {
	ix.dbPtrMutex.RLock()
	defer ix.dbPtrMutex.RUnlock()
	if ix.db == nil {
		return VideoRanges{}, errors.New("indexer not setup")
	}

	start := time.Now()
	segments, err := ix.getSegmentsAscTime(ctx)
	if err != nil {
		return VideoRanges{}, fmt.Errorf("failed to fetch segments for state: %w", err)
	}
	ix.logger.Debugf("TIMING: getting segments asc time took %v", time.Since(start))

	return getVideoRangesFromSegments(segments), nil
}

// getSegmentsAscTime is a helper function that retrieves all non-deleted segment data from the database,
// ordered by start time. Assumes the dbPtrMutex is read-locked.
func (ix *Indexer) getSegmentsAscTime(ctx context.Context) ([]segmentMetadata, error) {
	query := fmt.Sprintf(`
	SELECT file_name, start_time_unix, duration_ms, size_bytes, width, height, codec
	FROM %s
	WHERE deleted_at IS NULL
	ORDER BY start_time_unix ASC;
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

		if err := rows.Scan(&sm.FileName, &sm.StartTimeUnix, &sm.DurationMs, &sm.SizeBytes, &sm.Width, &sm.Height, &sm.Codec); err != nil {
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

// getVideoRangesFromSegments processes a slice of segment metadata to produce videoRanges.
func getVideoRangesFromSegments(segments []segmentMetadata) VideoRanges {
	var vr VideoRanges
	if len(segments) == 0 {
		return vr
	}

	var prevRange *VideoRange
	for _, s := range segments {
		vr.VideoCount++
		vr.TotalDurationMs += s.DurationMs
		vr.StorageUsedBytes += s.SizeBytes

		segmentStart := time.Unix(s.StartTimeUnix, 0)
		segmentEnd := segmentStart.Add(time.Duration(s.DurationMs) * time.Millisecond)

		if prevRange == nil {
			prevRange = &VideoRange{Start: segmentStart, End: segmentEnd}
		} else {
			if segmentStart.After(prevRange.End.Add(slopDuration)) {
				// make a new range as there is too big of a gap between the prev segment and the new segment
				vr.Ranges = append(vr.Ranges, *prevRange)
				prevRange = &VideoRange{Start: segmentStart, End: segmentEnd}
			} else {
				// extend range
				prevRange.End = segmentEnd
			}
		}
	}
	if prevRange != nil {
		vr.Ranges = append(vr.Ranges, *prevRange)
	}
	return vr
}

// Close closes the indexer and the underlying database.
func (ix *Indexer) Close() error {
	ix.dbPtrMutex.Lock()
	defer ix.dbPtrMutex.Unlock()
	if ix.db != nil {
		err := ix.db.Close()
		if err != nil {
			return fmt.Errorf("error closing index db: %w", err)
		}
		ix.db = nil
		ix.logger.Debug("indexer closed")
	}
	return nil
}
