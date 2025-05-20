package indexer

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	// SQLite driver.
	_ "github.com/mattn/go-sqlite3"
	"go.viam.com/rdk/logging"
	"go.viam.com/test"
)

const (
	defaultTestWidth  = 640
	defaultTestHeight = 480
	defaultTestCodec  = "h264"
)

// setupTestIndexer creates and initializes an indexer with an in-memory SQLite database for testing.
// It returns the indexer instance and a cleanup function.
func setupTestIndexer(t *testing.T, storagePath string) (*Indexer, func()) {
	t.Helper()
	idx := NewIndexer(storagePath, 1, logging.NewTestLogger(t))

	err := idx.setupDB(context.Background())
	test.That(t, err, test.ShouldBeNil)

	cleanup := func() {
		err := idx.Close()
		test.That(t, err, test.ShouldBeNil)
	}
	return idx, cleanup
}

// insertTestSegment inserts a segment record into the indexer's database.
// For the purpose of testing getVideoList, the fileName can be any unique string e.g. "seg1", "seg2" etc.
// getVideoList relies on the StartTimeUnix, DurationMs, and SizeBytes parameters
// directly from the DB and does not parse the fileName for timestamp for these tests.
func insertTestSegment(t *testing.T, idx *Indexer, fileName string, startTimeUnix, durationMs, sizeBytes int64) {
	t.Helper()
	query := fmt.Sprintf(
		"INSERT OR IGNORE INTO %s (file_name, start_time_unix, duration_ms, size_bytes, width, height, codec) VALUES (?, ?, ?, ?, ?, ?, ?);",
		segmentsTableName,
	)
	_, err := idx.db.Exec(query, fileName, startTimeUnix, durationMs, sizeBytes, defaultTestWidth, defaultTestHeight, defaultTestCodec)
	test.That(t, err, test.ShouldBeNil)
}

func TestGetVideoList(t *testing.T) {
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	testCases := []struct {
		name             string
		segmentsToInsert []segmentMetadata
		expectedRanges   VideoRanges
	}{
		{
			name:             "no segments",
			segmentsToInsert: []segmentMetadata{},
			expectedRanges: VideoRanges{
				StorageUsedBytes: 0,
				TotalDurationMs:  0,
				VideoCount:       0,
				Ranges:           []VideoRange{},
			},
		},
		{
			name: "single segment",
			segmentsToInsert: []segmentMetadata{
				{FileName: "seg1.mp4", StartTimeUnix: baseTime.Unix(), DurationMs: 10000, SizeBytes: 100, Width: defaultTestWidth, Height: defaultTestHeight, Codec: defaultTestCodec},
			},
			expectedRanges: VideoRanges{
				StorageUsedBytes: 100,
				TotalDurationMs:  10000,
				VideoCount:       1,
				Ranges: []VideoRange{
					{Start: baseTime, End: baseTime.Add(10 * time.Second)},
				},
			},
		},
		{
			name: "two contiguous segments (no gap)",
			segmentsToInsert: []segmentMetadata{
				{
					FileName:      "seg1.mp4",
					StartTimeUnix: baseTime.Unix(),
					DurationMs:    10000,
					SizeBytes:     100,
					Width:         defaultTestWidth, Height: defaultTestHeight, Codec: defaultTestCodec,
				}, // Ends at 00:00:10
				{
					FileName:      "seg2.mp4",
					StartTimeUnix: baseTime.Add(10 * time.Second).Unix(),
					DurationMs:    10000,
					SizeBytes:     150,
					Width:         defaultTestWidth, Height: defaultTestHeight, Codec: defaultTestCodec,
				}, // Starts at 00:00:10
			},
			expectedRanges: VideoRanges{
				StorageUsedBytes: 250,
				TotalDurationMs:  20000,
				VideoCount:       2,
				Ranges: []VideoRange{
					{
						Start: baseTime,
						End:   baseTime.Add(20 * time.Second),
					},
				},
			},
		},
		{
			name: "two contiguous segments with small gap (within slop duration)",
			segmentsToInsert: []segmentMetadata{
				{
					FileName:      "seg1.mp4",
					StartTimeUnix: baseTime.Unix(),
					DurationMs:    10000,
					SizeBytes:     100,
					Width:         defaultTestWidth, Height: defaultTestHeight, Codec: defaultTestCodec,
				}, // ends at 00:00:10
				{
					FileName:      "seg2.mp4",
					StartTimeUnix: baseTime.Add(12 * time.Second).Unix(),
					DurationMs:    10000,
					SizeBytes:     150,
					Width:         defaultTestWidth, Height: defaultTestHeight, Codec: defaultTestCodec,
				}, // starts at 00:00:12 (2s gap)
			},
			expectedRanges: VideoRanges{
				StorageUsedBytes: 250,
				TotalDurationMs:  20000,
				VideoCount:       2,
				Ranges: []VideoRange{
					{
						Start: baseTime,
						End:   baseTime.Add(12 * time.Second).Add(10 * time.Second), // merged range ends at end of seg2
					},
				},
			},
		},
		{
			name: "two segments exactly at slopDurationTest boundary",
			segmentsToInsert: []segmentMetadata{
				{
					FileName:      "seg1.mp4",
					StartTimeUnix: baseTime.Unix(),
					DurationMs:    10000,
					SizeBytes:     100,
					Width:         defaultTestWidth, Height: defaultTestHeight, Codec: defaultTestCodec,
				}, // ends at 00:00:10
				// Next segment starts at 00:00:15 (10s + 5s slop)
				{
					FileName:      "seg2.mp4",
					StartTimeUnix: baseTime.Add(10 * time.Second).Add(slopDuration).Unix(),
					DurationMs:    10000,
					SizeBytes:     150,
					Width:         defaultTestWidth, Height: defaultTestHeight, Codec: defaultTestCodec,
				},
			},
			expectedRanges: VideoRanges{
				StorageUsedBytes: 250,
				TotalDurationMs:  20000,
				VideoCount:       2,
				Ranges: []VideoRange{
					{Start: baseTime, End: baseTime.Add(10 * time.Second).Add(slopDuration).Add(10 * time.Second)},
				},
			},
		},
		{
			name: "two non-contiguous segments (gap > slopDurationTest at second precision)",
			segmentsToInsert: []segmentMetadata{
				{
					FileName:      "seg1.mp4",
					StartTimeUnix: baseTime.Unix(),
					DurationMs:    10000,
					SizeBytes:     100,
					Width:         defaultTestWidth, Height: defaultTestHeight, Codec: defaultTestCodec,
				},
				{
					FileName:      "seg2.mp4",
					StartTimeUnix: baseTime.Add(16 * time.Second).Unix(),
					DurationMs:    10000,
					SizeBytes:     150,
					Width:         defaultTestWidth, Height: defaultTestHeight, Codec: defaultTestCodec,
				},
			},
			expectedRanges: VideoRanges{
				StorageUsedBytes: 250,
				TotalDurationMs:  20000,
				VideoCount:       2,
				Ranges: []VideoRange{
					{
						Start: baseTime,
						End:   baseTime.Add(10 * time.Second),
					},
					{
						Start: baseTime.Add(16 * time.Second),
						End:   baseTime.Add(16 * time.Second).Add(10 * time.Second),
					},
				},
			},
		},
		{
			name: "multiple segments, mixed contiguity",
			segmentsToInsert: []segmentMetadata{
				{
					FileName:      "seg1.mp4",
					StartTimeUnix: baseTime.Unix(),
					DurationMs:    10000,
					SizeBytes:     100,
					Width:         defaultTestWidth, Height: defaultTestHeight, Codec: defaultTestCodec,
				}, // R1: 00:00:00 - 00:00:10
				{
					FileName:      "seg2.mp4",
					StartTimeUnix: baseTime.Add(10 * time.Second).Unix(),
					DurationMs:    10000,
					SizeBytes:     150,
					Width:         defaultTestWidth, Height: defaultTestHeight, Codec: defaultTestCodec,
				}, // R1: 00:00:10 - 00:00:20 (contig)
				{
					FileName:      "seg3.mp4",
					StartTimeUnix: baseTime.Add(30 * time.Second).Unix(),
					DurationMs:    10000,
					SizeBytes:     200,
					Width:         defaultTestWidth, Height: defaultTestHeight, Codec: defaultTestCodec,
				}, // R2: 00:00:30 - 00:00:40 (sep)
				{
					FileName:      "seg4.mp4",
					StartTimeUnix: baseTime.Add(42 * time.Second).Unix(),
					DurationMs:    10000,
					SizeBytes:     250,
					Width:         defaultTestWidth, Height: defaultTestHeight, Codec: defaultTestCodec,
				}, // R2: 00:00:42 - 00:00:52 (contig with R2 by 2s gap < 5s slop)
				{
					FileName:      "seg5.mp4",
					StartTimeUnix: baseTime.Add(60 * time.Second).Unix(),
					DurationMs:    10000,
					SizeBytes:     300,
					Width:         defaultTestWidth, Height: defaultTestHeight, Codec: defaultTestCodec,
				}, // R3: 00:01:00 - 00:01:10 (sep)
			},
			expectedRanges: VideoRanges{
				StorageUsedBytes: 100 + 150 + 200 + 250 + 300,
				TotalDurationMs:  10000 * 5,
				VideoCount:       5,
				Ranges: []VideoRange{
					{
						Start: baseTime,
						End:   baseTime.Add(20 * time.Second),
					}, // seg1 & seg2
					{
						Start: baseTime.Add(30 * time.Second),
						End:   baseTime.Add(42 * time.Second).Add(10 * time.Second),
					}, // seg3 & seg4
					{
						Start: baseTime.Add(60 * time.Second),
						End:   baseTime.Add(70 * time.Second),
					}, // seg5
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			idx, cleanup := setupTestIndexer(t, t.TempDir())
			defer cleanup()

			for _, seg := range tc.segmentsToInsert {
				insertTestSegment(t, idx, seg.FileName, seg.StartTimeUnix, seg.DurationMs, seg.SizeBytes)
			}

			resultRanges, err := idx.GetVideoList(context.Background())
			test.That(t, err, test.ShouldBeNil)

			test.That(t, resultRanges.StorageUsedBytes, test.ShouldEqual, tc.expectedRanges.StorageUsedBytes)
			test.That(t, resultRanges.TotalDurationMs, test.ShouldEqual, tc.expectedRanges.TotalDurationMs)
			test.That(t, resultRanges.VideoCount, test.ShouldEqual, tc.expectedRanges.VideoCount)

			test.That(t, len(resultRanges.Ranges), test.ShouldEqual, len(tc.expectedRanges.Ranges))

			for i := range tc.expectedRanges.Ranges {
				expectedR := tc.expectedRanges.Ranges[i]
				actualR := resultRanges.Ranges[i]

				test.That(t, actualR.Start, test.ShouldEqual, expectedR.Start)
				test.That(t, actualR.End, test.ShouldEqual, expectedR.End)
			}
		})
	}
}

func TestGetVideoRangesFromSegments(t *testing.T) {
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	testCases := []struct {
		name           string
		segments       []segmentMetadata
		expectedRanges VideoRanges
	}{
		{
			name:           "empty segments",
			segments:       []segmentMetadata{},
			expectedRanges: VideoRanges{},
		},
		{
			name: "zero duration segment",
			segments: []segmentMetadata{
				{FileName: "seg1.mp4", StartTimeUnix: baseTime.Unix(), DurationMs: 0, SizeBytes: 100},
			},
			expectedRanges: VideoRanges{
				StorageUsedBytes: 100,
				TotalDurationMs:  0,
				VideoCount:       1,
				Ranges: []VideoRange{
					{Start: baseTime, End: baseTime},
				},
			},
		},
		{
			name: "multiple slop duration gaps",
			segments: []segmentMetadata{
				{FileName: "seg1.mp4", StartTimeUnix: baseTime.Unix(), DurationMs: 10000, SizeBytes: 100},
				{FileName: "seg2.mp4", StartTimeUnix: baseTime.Add(20 * time.Second).Unix(), DurationMs: 10000, SizeBytes: 150},
				{FileName: "seg3.mp4", StartTimeUnix: baseTime.Add(40 * time.Second).Unix(), DurationMs: 10000, SizeBytes: 200},
			},
			expectedRanges: VideoRanges{
				StorageUsedBytes: 450,
				TotalDurationMs:  30000,
				VideoCount:       3,
				Ranges: []VideoRange{
					{Start: baseTime, End: baseTime.Add(10 * time.Second)},
					{Start: baseTime.Add(20 * time.Second), End: baseTime.Add(30 * time.Second)},
					{Start: baseTime.Add(40 * time.Second), End: baseTime.Add(50 * time.Second)},
				},
			},
		},
		{
			name: "merge contiguous, gap splits",
			segments: []segmentMetadata{
				// seg1: 0s-10s
				{FileName: "seg1.mp4", StartTimeUnix: baseTime.Unix(), DurationMs: 10000, SizeBytes: 100},
				// seg2: 10s-20s (contiguous with seg1)
				{FileName: "seg2.mp4", StartTimeUnix: baseTime.Add(10 * time.Second).Unix(), DurationMs: 10000, SizeBytes: 150},
				// seg3: 30s-40s (gap > slopDuration from seg2)
				{FileName: "seg3.mp4", StartTimeUnix: baseTime.Add(30 * time.Second).Unix(), DurationMs: 10000, SizeBytes: 200},
			},
			expectedRanges: VideoRanges{
				StorageUsedBytes: 450,
				TotalDurationMs:  30000,
				VideoCount:       3,
				Ranges: []VideoRange{
					{Start: baseTime, End: baseTime.Add(20 * time.Second)},
					{Start: baseTime.Add(30 * time.Second), End: baseTime.Add(40 * time.Second)},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resultRanges := getVideoRangesFromSegments(tc.segments)

			test.That(t, resultRanges.StorageUsedBytes, test.ShouldEqual, tc.expectedRanges.StorageUsedBytes)
			test.That(t, resultRanges.TotalDurationMs, test.ShouldEqual, tc.expectedRanges.TotalDurationMs)
			test.That(t, resultRanges.VideoCount, test.ShouldEqual, tc.expectedRanges.VideoCount)

			test.That(t, len(resultRanges.Ranges), test.ShouldEqual, len(tc.expectedRanges.Ranges))

			for i := range tc.expectedRanges.Ranges {
				expectedR := tc.expectedRanges.Ranges[i]
				actualR := resultRanges.Ranges[i]

				test.That(t, actualR.Start, test.ShouldEqual, expectedR.Start)
				test.That(t, actualR.End, test.ShouldEqual, expectedR.End)
			}
		})
	}
}

// createDummyFile creates a file with the given name and size in the specified directory.
func createDummyFile(t *testing.T, dirPath, fileName string, sizeBytes int64) error {
	t.Helper()
	fullPath := filepath.Join(dirPath, fileName)
	data := make([]byte, sizeBytes)
	return os.WriteFile(fullPath, data, 0o644)
}

func TestDeletions(t *testing.T) {
	storagePath := t.TempDir()
	idx, cleanup := setupTestIndexer(t, storagePath)
	defer cleanup()

	megabyte := int64(1024 * 1024)
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	filesToTest := []struct {
		name        string
		timestamp   time.Time
		sizeBytes   int64
		durationMs  int64
		toBeDeleted bool
	}{
		{fmt.Sprintf("%d%s", baseTime.Unix(), videoFileSuffix), baseTime, 600 * megabyte, 30000, true},
		{fmt.Sprintf("%d%s", baseTime.Add(1*time.Minute).Unix(), videoFileSuffix), baseTime.Add(1 * time.Minute), 600 * megabyte, 30000, true},
		{fmt.Sprintf("%d%s", baseTime.Add(2*time.Minute).Unix(), videoFileSuffix), baseTime.Add(2 * time.Minute), 600 * megabyte, 30000, false},
	}

	// Create the dummy files and insert their metadata into the DB
	for _, f := range filesToTest {
		err := createDummyFile(t, storagePath, f.name, f.sizeBytes)
		test.That(t, err, test.ShouldBeNil)

		query := fmt.Sprintf(
			"INSERT OR IGNORE INTO %s (file_name, start_time_unix, duration_ms, size_bytes, width, height, codec) VALUES (?, ?, ?, ?, ?, ?, ?);",
			segmentsTableName,
		)
		_, err = idx.db.Exec(query, f.name, f.timestamp.Unix(), f.durationMs, f.sizeBytes, defaultTestWidth, defaultTestHeight, defaultTestCodec)
		test.That(t, err, test.ShouldBeNil)
	}

	idx.refreshIndexAndStorage(context.Background())

	// Assertions: check disk and DB state
	for _, f := range filesToTest {
		fullPath := filepath.Join(storagePath, f.name)
		_, statErr := os.Stat(fullPath)

		var dbFileName string
		dbQueryErr := idx.db.QueryRow("SELECT file_name FROM segments WHERE file_name = ?", f.name).Scan(&dbFileName)

		if f.toBeDeleted {
			test.That(t, os.IsNotExist(statErr), test.ShouldBeTrue)
			test.That(t, dbQueryErr, test.ShouldBeError, sql.ErrNoRows)
		} else {
			test.That(t, statErr, test.ShouldBeNil)
			test.That(t, dbQueryErr, test.ShouldBeNil)

			var deletedAt sql.NullTime
			dbScanErr := idx.db.QueryRow("SELECT deleted_at FROM segments WHERE file_name = ?", f.name).Scan(&deletedAt)
			test.That(t, dbScanErr, test.ShouldBeNil)
			test.That(t, deletedAt.Valid, test.ShouldBeFalse)
		}
	}
}

func TestManuallyDeletedFile(t *testing.T) {
	storagePath := t.TempDir()
	idx, cleanup := setupTestIndexer(t, storagePath)
	defer cleanup()

	ctx := context.Background()
	dummyFileName := "manually_deleted_segment.mp4"
	dummyFilePath := filepath.Join(storagePath, dummyFileName)
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create a file and index it
	err := createDummyFile(t, storagePath, dummyFileName, 1024)
	test.That(t, err, test.ShouldBeNil)

	query := fmt.Sprintf(
		"INSERT INTO %s (file_name, start_time_unix, duration_ms, size_bytes, width, height, codec) VALUES (?, ?, ?, ?, ?, ?, ?);",
		segmentsTableName,
	)
	_, err = idx.db.ExecContext(ctx, query, dummyFileName, baseTime.Unix(), 10000, 1024, defaultTestWidth, defaultTestHeight, defaultTestCodec)
	test.That(t, err, test.ShouldBeNil)

	// Verify it's in the DB and not marked as deleted
	var deletedAt sql.NullTime
	err = idx.db.QueryRowContext(ctx, "SELECT deleted_at FROM segments WHERE file_name = ?", dummyFileName).Scan(&deletedAt)
	test.That(t, err, test.ShouldBeNil)
	test.That(t, deletedAt.Valid, test.ShouldBeFalse)

	// Delete the file from disk
	err = os.Remove(dummyFilePath)
	test.That(t, err, test.ShouldBeNil)

	// Run the indexer's refresh logic
	idx.refreshIndexAndStorage(ctx)

	// Assert that the segments table is now empty (record was deleted)
	var count int
	err = idx.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM segments").Scan(&count)
	test.That(t, err, test.ShouldBeNil)
	test.That(t, count, test.ShouldEqual, 0)
}
