package cdc

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
)

const (
	segmentCatalogFilename   = "segments.catalog"
	segmentCatalogTemp       = "segments.catalog.tmp"
	segmentPruneFilename     = "segments.pruned"
	segmentPruneTemp         = "segments.pruned.tmp"
	segmentCatalogVersion    = uint32(1)
	segmentCatalogHeaderSize = 16
	segmentPruneRecordSize   = 28
	segmentSeekInterval      = int64(4 << 20)
)

var segmentCatalogMagic = [4]byte{'P', 'G', 'S', 'C'}
var segmentPruneMagic = [4]byte{'P', 'G', 'S', 'P'}

// SegmentSeekPoint describes a safe frame boundary. PreviousCommit and
// PreviousEnd are the ordering state immediately before Offset.
type SegmentSeekPoint struct {
	Offset         int64
	PreviousCommit LSN
	PreviousEnd    LSN
}

type diskSegmentCatalog struct {
	Version       uint32               `json:"version"`
	Generation    uint64               `json:"generation"`
	PrunedThrough uint64               `json:"pruned_through"`
	Segments      []diskCatalogSegment `json:"segments"`
}

type diskCatalogSegment struct {
	Name        string                 `json:"name"`
	StartCommit uint64                 `json:"start_commit"`
	LastCommit  uint64                 `json:"last_commit"`
	LastEnd     uint64                 `json:"last_end"`
	Size        int64                  `json:"size"`
	SeekPoints  []diskCatalogSeekPoint `json:"seek_points,omitempty"`
}

type diskCatalogSeekPoint struct {
	Offset         int64  `json:"offset"`
	PreviousCommit uint64 `json:"previous_commit"`
	PreviousEnd    uint64 `json:"previous_end"`
}

func segmentCatalogPath(directory string) string {
	return filepath.Join(directory, segmentCatalogFilename)
}

func segmentPrunePath(directory string) string {
	return filepath.Join(directory, segmentPruneFilename)
}

func loadPruneWatermark(directory string) (LSN, uint64, bool, error) {
	data, err := os.ReadFile(segmentPrunePath(directory))
	if errors.Is(err, os.ErrNotExist) {
		return 0, 0, false, nil
	}
	if err != nil {
		return 0, 0, false, fmt.Errorf("cdc: read pruned-prefix watermark: %w", err)
	}
	if len(data) != segmentPruneRecordSize ||
		[4]byte(data[:4]) != segmentPruneMagic ||
		binary.LittleEndian.Uint32(data[4:8]) != segmentCatalogVersion {
		return 0, 0, false, errors.New("cdc: invalid pruned-prefix watermark")
	}
	expected := binary.LittleEndian.Uint32(data[24:28])
	if actual := crc32.Checksum(data[8:24], castagnoliTable); actual != expected {
		return 0, 0, false, fmt.Errorf(
			"cdc: pruned-prefix watermark checksum mismatch: got %08x, want %08x",
			actual, expected,
		)
	}
	return LSN(binary.LittleEndian.Uint64(data[8:16])),
		binary.LittleEndian.Uint64(data[16:24]), true, nil
}

func persistPruneWatermark(
	directory string,
	generation uint64,
	prunedThrough LSN,
) error {
	var data [segmentPruneRecordSize]byte
	copy(data[:4], segmentPruneMagic[:])
	binary.LittleEndian.PutUint32(data[4:8], segmentCatalogVersion)
	binary.LittleEndian.PutUint64(data[8:16], uint64(prunedThrough))
	binary.LittleEndian.PutUint64(data[16:24], generation)
	binary.LittleEndian.PutUint32(
		data[24:28], crc32.Checksum(data[8:24], castagnoliTable),
	)
	tempPath := filepath.Join(directory, segmentPruneTemp)
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o640)
	if err != nil {
		return fmt.Errorf("cdc: create pruned-prefix temporary file: %w", err)
	}
	if err := writeFull(file, data[:]); err != nil {
		return errors.Join(
			fmt.Errorf("cdc: write pruned-prefix watermark: %w", err),
			file.Close(),
		)
	}
	if err := file.Sync(); err != nil {
		return errors.Join(
			fmt.Errorf("cdc: sync pruned-prefix watermark: %w", err),
			file.Close(),
		)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("cdc: close pruned-prefix watermark: %w", err)
	}
	if err := os.Rename(tempPath, segmentPrunePath(directory)); err != nil {
		return fmt.Errorf("cdc: publish pruned-prefix watermark: %w", err)
	}
	if err := syncDirectory(directory); err != nil {
		return fmt.Errorf("cdc: sync published pruned-prefix watermark: %w", err)
	}
	return nil
}

func loadDiskSegmentCatalog(directory string) (diskSegmentCatalog, bool, error) {
	file, err := os.Open(segmentCatalogPath(directory))
	if errors.Is(err, os.ErrNotExist) {
		return diskSegmentCatalog{}, false, nil
	}
	if err != nil {
		return diskSegmentCatalog{}, false, fmt.Errorf("cdc: open segment catalog: %w", err)
	}
	defer file.Close()
	var header [segmentCatalogHeaderSize]byte
	if _, err := io.ReadFull(file, header[:]); err != nil {
		return diskSegmentCatalog{}, false, fmt.Errorf("cdc: read segment catalog header: %w", err)
	}
	if [4]byte(header[:4]) != segmentCatalogMagic {
		return diskSegmentCatalog{}, false, errors.New("cdc: invalid segment catalog magic")
	}
	version := binary.LittleEndian.Uint32(header[4:8])
	if version != segmentCatalogVersion {
		return diskSegmentCatalog{}, false, fmt.Errorf(
			"cdc: unsupported segment catalog version %d", version,
		)
	}
	length := binary.LittleEndian.Uint32(header[8:12])
	if uint64(length) > maxPayloadSize {
		return diskSegmentCatalog{}, false, fmt.Errorf(
			"cdc: segment catalog payload exceeds %d bytes", maxPayloadSize,
		)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(file, payload); err != nil {
		return diskSegmentCatalog{}, false, fmt.Errorf("cdc: read segment catalog payload: %w", err)
	}
	var trailing [1]byte
	if n, err := file.Read(trailing[:]); err != io.EOF || n != 0 {
		if err == nil {
			err = errors.New("trailing bytes")
		}
		return diskSegmentCatalog{}, false, fmt.Errorf("cdc: invalid segment catalog: %w", err)
	}
	expected := binary.LittleEndian.Uint32(header[12:16])
	if actual := crc32.Checksum(payload, castagnoliTable); actual != expected {
		return diskSegmentCatalog{}, false, fmt.Errorf(
			"cdc: segment catalog checksum mismatch: got %08x, want %08x",
			actual, expected,
		)
	}
	var catalog diskSegmentCatalog
	if err := json.Unmarshal(payload, &catalog); err != nil {
		return diskSegmentCatalog{}, false, fmt.Errorf("cdc: decode segment catalog: %w", err)
	}
	if catalog.Version != segmentCatalogVersion {
		return diskSegmentCatalog{}, false, fmt.Errorf(
			"cdc: segment catalog payload version %d does not match", catalog.Version,
		)
	}
	return catalog, true, nil
}

func persistDiskSegmentCatalog(
	directory string,
	generation uint64,
	prunedThrough LSN,
	segments []SegmentRange,
) error {
	catalog := diskSegmentCatalog{
		Version:       segmentCatalogVersion,
		Generation:    generation,
		PrunedThrough: uint64(prunedThrough),
		Segments:      make([]diskCatalogSegment, len(segments)),
	}
	for i, segment := range segments {
		entry := diskCatalogSegment{
			Name:        filepath.Base(segment.Path),
			StartCommit: uint64(segment.StartCommit),
			LastCommit:  uint64(segment.LastCommit),
			LastEnd:     uint64(segment.LastEnd),
			Size:        segment.ValidatedSize,
			SeekPoints:  make([]diskCatalogSeekPoint, len(segment.SeekPoints)),
		}
		for j, point := range segment.SeekPoints {
			entry.SeekPoints[j] = diskCatalogSeekPoint{
				Offset:         point.Offset,
				PreviousCommit: uint64(point.PreviousCommit),
				PreviousEnd:    uint64(point.PreviousEnd),
			}
		}
		catalog.Segments[i] = entry
	}
	payload, err := json.Marshal(catalog)
	if err != nil {
		return fmt.Errorf("cdc: encode segment catalog: %w", err)
	}
	var header [segmentCatalogHeaderSize]byte
	copy(header[:4], segmentCatalogMagic[:])
	binary.LittleEndian.PutUint32(header[4:8], segmentCatalogVersion)
	binary.LittleEndian.PutUint32(header[8:12], uint32(len(payload)))
	binary.LittleEndian.PutUint32(
		header[12:16], crc32.Checksum(payload, castagnoliTable),
	)
	tempPath := filepath.Join(directory, segmentCatalogTemp)
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o640)
	if err != nil {
		return fmt.Errorf("cdc: create segment catalog temporary file: %w", err)
	}
	closeWithError := func(writeErr error) error {
		return errors.Join(writeErr, file.Close())
	}
	if err := writeFull(file, header[:]); err != nil {
		return closeWithError(fmt.Errorf("cdc: write segment catalog header: %w", err))
	}
	if err := writeFull(file, payload); err != nil {
		return closeWithError(fmt.Errorf("cdc: write segment catalog payload: %w", err))
	}
	if err := file.Sync(); err != nil {
		return closeWithError(fmt.Errorf("cdc: sync segment catalog: %w", err))
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("cdc: close segment catalog: %w", err)
	}
	if err := os.Rename(tempPath, segmentCatalogPath(directory)); err != nil {
		return fmt.Errorf("cdc: publish segment catalog: %w", err)
	}
	if err := syncDirectory(directory); err != nil {
		return fmt.Errorf("cdc: sync published segment catalog: %w", err)
	}
	return nil
}

func diskCatalogRanges(
	directory string,
	catalog diskSegmentCatalog,
) ([]SegmentRange, error) {
	ranges := make([]SegmentRange, len(catalog.Segments))
	for i, entry := range catalog.Segments {
		start, partial, ok := parseSegmentName(entry.Name)
		if !ok || partial || start != LSN(entry.StartCommit) {
			return nil, fmt.Errorf("cdc: invalid cataloged segment name %q", entry.Name)
		}
		if filepath.Base(entry.Name) != entry.Name {
			return nil, fmt.Errorf("cdc: cataloged segment name %q is not a base name", entry.Name)
		}
		points := make([]SegmentSeekPoint, len(entry.SeekPoints))
		for j, point := range entry.SeekPoints {
			points[j] = SegmentSeekPoint{
				Offset:         point.Offset,
				PreviousCommit: LSN(point.PreviousCommit),
				PreviousEnd:    LSN(point.PreviousEnd),
			}
		}
		ranges[i] = SegmentRange{
			Path:          filepath.Join(directory, entry.Name),
			StartCommit:   start,
			LastCommit:    LSN(entry.LastCommit),
			LastEnd:       LSN(entry.LastEnd),
			ValidatedSize: entry.Size,
			SeekPoints:    points,
		}
	}
	if err := validateSegmentRanges(ranges); err != nil {
		return nil, err
	}
	return ranges, nil
}

func validateSegmentRanges(ranges []SegmentRange) error {
	var previousCommit, previousEnd LSN
	for i, segment := range ranges {
		if segment.StartCommit == 0 ||
			segment.LastCommit < segment.StartCommit ||
			segment.LastEnd < segment.LastCommit ||
			segment.ValidatedSize < frameHeaderSize {
			return fmt.Errorf(
				"cdc: invalid cataloged range for %s", filepath.Base(segment.Path),
			)
		}
		if i > 0 &&
			(segment.StartCommit <= previousCommit ||
				segment.LastCommit <= previousCommit ||
				segment.LastEnd <= previousEnd) {
			return fmt.Errorf(
				"cdc: cataloged segment %s does not follow its predecessor",
				filepath.Base(segment.Path),
			)
		}
		var pointOffset int64 = -1
		var pointCommit, pointEnd LSN
		for _, point := range segment.SeekPoints {
			if point.Offset < 0 ||
				point.Offset >= segment.ValidatedSize ||
				point.Offset <= pointOffset ||
				point.PreviousCommit < pointCommit ||
				point.PreviousEnd < pointEnd ||
				point.PreviousEnd > segment.LastEnd {
				return fmt.Errorf(
					"cdc: invalid seek points for %s", filepath.Base(segment.Path),
				)
			}
			pointOffset = point.Offset
			pointCommit = point.PreviousCommit
			pointEnd = point.PreviousEnd
		}
		previousCommit = segment.LastCommit
		previousEnd = segment.LastEnd
	}
	return nil
}

func cloneSegmentRanges(segments []SegmentRange) []SegmentRange {
	cloned := make([]SegmentRange, len(segments))
	for i, segment := range segments {
		cloned[i] = segment
		cloned[i].SeekPoints = slices.Clone(segment.SeekPoints)
	}
	return cloned
}

func scanFinalizedSegments(
	segments []segmentFile,
	tracker *recoveryTracker,
) ([]scanResult, error) {
	if len(segments) == 0 {
		return nil, nil
	}
	workers := min(len(segments), max(1, min(runtime.GOMAXPROCS(0), 4)))
	results := make([]scanResult, len(segments))
	jobs := make(chan int)
	errs := make(chan error, 1)
	var group sync.WaitGroup
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			for index := range jobs {
				scan, err := scanSegmentObserved(
					segments[index].path, false, 0, 0, nil, tracker.scanned,
				)
				if err != nil {
					select {
					case errs <- err:
					default:
					}
					continue
				}
				results[index] = scan
				tracker.segmentScanned()
			}
		}()
	}
	for index := range segments {
		select {
		case jobs <- index:
		case err := <-errs:
			close(jobs)
			group.Wait()
			return nil, err
		}
	}
	close(jobs)
	group.Wait()
	select {
	case err := <-errs:
		return nil, err
	default:
		return results, nil
	}
}
