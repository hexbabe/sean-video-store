package videostore

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	// SQLite driver.
	_ "github.com/mattn/go-sqlite3"
	"go.viam.com/rdk/logging"
	"go.viam.com/test"
)

// setupTestIndexer creates and initializes an indexer with an in-memory SQLite database for testing.
// It returns the indexer instance and a cleanup function.
func setupTestIndexer(t *testing.T) (*indexer, func()) {
	t.Helper()
	idx := newIndexer("", 1, logging.NewTestLogger(t))

	// Explicitly set dbPath to an in-memory database for actual testing to avoid disk I/O.
	inMemoryDBPath := fmt.Sprintf("file:%s?mode=memory&cache=shared", filepath.Join("", "test_index.sqlite.db"))
	db, err := sql.Open("sqlite3", inMemoryDBPath)
	test.That(t, err, test.ShouldBeNil)

	idx.db = db
	idx.dbPath = inMemoryDBPath

	err = idx.initializeDB()
	test.That(t, err, test.ShouldBeNil)
	idx.setupDone = true

	cleanup := func() {
		err := idx.close()
		test.That(t, err, test.ShouldBeNil)
	}
	return idx, cleanup
}

// insertTestSegment inserts a segment record into the indexer's database.
func insertTestSegment(t *testing.T, idx *indexer, filePath string, startTimeUnix, durationMs, sizeBytes int64) {
	t.Helper()
	query := fmt.Sprintf(
		"INSERT INTO %s (file_path, start_time_unix, duration_ms, size_bytes) VALUES (?, ?, ?, ?);",
		segmentsTableName,
	)
	_, err := idx.db.Exec(query, filePath, startTimeUnix, durationMs, sizeBytes)
	test.That(t, err, test.ShouldBeNil)
}

func TestGetVideoList(t *testing.T) {
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	testCases := []struct {
		name             string
		segmentsToInsert []segmentMetadata
		expectedRanges   videoRanges
	}{
		{
			name:             "no segments",
			segmentsToInsert: []segmentMetadata{},
			expectedRanges: videoRanges{
				TotalSizeBytes:  0,
				TotalDurationMs: 0,
				VideoCount:      0,
				Ranges:          []videoRange{},
			},
		},
		{
			name: "single segment",
			segmentsToInsert: []segmentMetadata{
				{FilePath: "seg1.mp4", StartTimeUnix: baseTime.Unix(), DurationMs: 10000, SizeBytes: 100},
			},
			expectedRanges: videoRanges{
				TotalSizeBytes:  100,
				TotalDurationMs: 10000,
				VideoCount:      1,
				Ranges: []videoRange{
					{Start: baseTime, End: baseTime.Add(10 * time.Second)},
				},
			},
		},
		{
			name: "two contiguous segments (no gap)",
			segmentsToInsert: []segmentMetadata{
				{
					FilePath:      "seg1.mp4",
					StartTimeUnix: baseTime.Unix(),
					DurationMs:    10000,
					SizeBytes:     100,
				}, // Ends at 00:00:10
				{
					FilePath:      "seg2.mp4",
					StartTimeUnix: baseTime.Add(10 * time.Second).Unix(),
					DurationMs:    10000,
					SizeBytes:     150,
				}, // Starts at 00:00:10
			},
			expectedRanges: videoRanges{
				TotalSizeBytes:  250,
				TotalDurationMs: 20000,
				VideoCount:      2,
				Ranges: []videoRange{
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
					FilePath:      "seg1.mp4",
					StartTimeUnix: baseTime.Unix(),
					DurationMs:    10000,
					SizeBytes:     100,
				}, // ends at 00:00:10
				{
					FilePath:      "seg2.mp4",
					StartTimeUnix: baseTime.Add(12 * time.Second).Unix(),
					DurationMs:    10000,
					SizeBytes:     150,
				}, // starts at 00:00:12 (2s gap)
			},
			expectedRanges: videoRanges{
				TotalSizeBytes:  250,
				TotalDurationMs: 20000,
				VideoCount:      2,
				Ranges: []videoRange{
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
					FilePath:      "seg1.mp4",
					StartTimeUnix: baseTime.Unix(),
					DurationMs:    10000,
					SizeBytes:     100,
				}, // ends at 00:00:10
				// Next segment starts at 00:00:15 (10s + 5s slop)
				{
					FilePath:      "seg2.mp4",
					StartTimeUnix: baseTime.Add(10 * time.Second).Add(slopDuration).Unix(),
					DurationMs:    10000,
					SizeBytes:     150,
				},
			},
			expectedRanges: videoRanges{
				TotalSizeBytes:  250,
				TotalDurationMs: 20000,
				VideoCount:      2,
				Ranges: []videoRange{
					{Start: baseTime, End: baseTime.Add(10 * time.Second).Add(slopDuration).Add(10 * time.Second)},
				},
			},
		},
		{
			name: "two non-contiguous segments (gap > slopDurationTest at second precision)",
			segmentsToInsert: []segmentMetadata{
				{
					FilePath:      "seg1.mp4",
					StartTimeUnix: baseTime.Unix(),
					DurationMs:    10000,
					SizeBytes:     100,
				},
				{
					FilePath:      "seg2.mp4",
					StartTimeUnix: baseTime.Add(16 * time.Second).Unix(),
					DurationMs:    10000,
					SizeBytes:     150,
				},
			},
			expectedRanges: videoRanges{
				TotalSizeBytes:  250,
				TotalDurationMs: 20000,
				VideoCount:      2,
				Ranges: []videoRange{
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
					FilePath:      "seg1.mp4",
					StartTimeUnix: baseTime.Unix(),
					DurationMs:    10000,
					SizeBytes:     100,
				}, // R1: 00:00:00 - 00:00:10
				{
					FilePath:      "seg2.mp4",
					StartTimeUnix: baseTime.Add(10 * time.Second).Unix(),
					DurationMs:    10000,
					SizeBytes:     150,
				}, // R1: 00:00:10 - 00:00:20 (contig)
				{
					FilePath:      "seg3.mp4",
					StartTimeUnix: baseTime.Add(30 * time.Second).Unix(),
					DurationMs:    10000,
					SizeBytes:     200,
				}, // R2: 00:00:30 - 00:00:40 (sep)
				{
					FilePath:      "seg4.mp4",
					StartTimeUnix: baseTime.Add(42 * time.Second).Unix(),
					DurationMs:    10000,
					SizeBytes:     250,
				}, // R2: 00:00:42 - 00:00:52 (contig with R2 by 2s gap < 5s slop)
				{
					FilePath:      "seg5.mp4",
					StartTimeUnix: baseTime.Add(60 * time.Second).Unix(),
					DurationMs:    10000,
					SizeBytes:     300,
				}, // R3: 00:01:00 - 00:01:10 (sep)
			},
			expectedRanges: videoRanges{
				TotalSizeBytes:  100 + 150 + 200 + 250 + 300,
				TotalDurationMs: 10000 * 5,
				VideoCount:      5,
				Ranges: []videoRange{
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
			idx, cleanup := setupTestIndexer(t)
			defer cleanup()

			for _, seg := range tc.segmentsToInsert {
				insertTestSegment(t, idx, seg.FilePath, seg.StartTimeUnix, seg.DurationMs, seg.SizeBytes)
			}

			resultRanges, err := idx.getVideoList(context.Background())
			test.That(t, err, test.ShouldBeNil)

			test.That(t, resultRanges.TotalSizeBytes, test.ShouldEqual, tc.expectedRanges.TotalSizeBytes)
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
