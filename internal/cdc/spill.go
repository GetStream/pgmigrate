package cdc

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
)

const spillFilePrefix = "pgmigrate-cdc-spill-"

var cleanedSpillDirectories sync.Map

// TransactionSpill owns a locked temporary file containing only the
// deterministic encoded Change sequence (without the collection count).
type TransactionSpill struct {
	mu          sync.Mutex
	path        string
	file        *os.File
	changeCount uint32
	changeBytes uint64
	version     byte
}

func (spill *TransactionSpill) Path() string {
	if spill == nil {
		return ""
	}
	spill.mu.Lock()
	defer spill.mu.Unlock()
	return spill.path
}

func newTransactionSpill(directory string) (*TransactionSpill, error) {
	file, err := os.CreateTemp(directory, spillFilePrefix+"*.tmp")
	if err != nil {
		return nil, fmt.Errorf("cdc: create transaction spill: %w", err)
	}
	if err := lockSpill(file, false); err != nil {
		_ = file.Close()
		_ = os.Remove(file.Name())
		return nil, fmt.Errorf("cdc: lock transaction spill: %w", err)
	}
	if err := syncDirectory(directory); err != nil {
		_ = unlockSpill(file)
		_ = file.Close()
		_ = os.Remove(file.Name())
		return nil, fmt.Errorf("cdc: fsync transaction spill entry: %w", err)
	}
	return &TransactionSpill{path: file.Name(), file: file, version: payloadVersion}, nil
}

func (spill *TransactionSpill) forEachChange(visit func(Change) error) error {
	reader, changeBytes, changeCount, err := spill.reader()
	if err != nil {
		return err
	}
	limited := &io.LimitedReader{R: reader, N: int64(changeBytes)}
	for i := uint32(0); i < changeCount; i++ {
		change, err := decodeChangeReader(limited, spill.version)
		if err != nil {
			return fmt.Errorf("cdc: decode spilled change %d: %w", i, err)
		}
		if err := visit(change); err != nil {
			return err
		}
	}
	if limited.N != 0 {
		return fmt.Errorf("cdc: spilled changes have %d trailing bytes", limited.N)
	}
	return nil
}

func (tx *Transaction) materializeChanges() error {
	if tx == nil || tx.Spill == nil {
		return nil
	}
	changes := make([]Change, 0, tx.Spill.changeCount)
	if err := tx.Spill.forEachChange(func(change Change) error {
		changes = append(changes, change)
		return nil
	}); err != nil {
		return err
	}
	if err := tx.CleanupSpill(); err != nil {
		return err
	}
	tx.Changes = changes
	return nil
}

func (spill *TransactionSpill) appendChange(change *Change) error {
	spill.mu.Lock()
	defer spill.mu.Unlock()
	if spill.file == nil {
		return errors.New("cdc: append to closed transaction spill")
	}
	changeSize, err := encodedChangeSize(change)
	if err != nil {
		return err
	}
	if spill.changeBytes > uint64(^uint32(0))-changeSize {
		return errors.New("cdc: spilled changes exceed uint32 payload limit")
	}
	written, err := writeChangeTo(spill.file, change)
	if err != nil {
		return fmt.Errorf("cdc: write transaction spill: %w", err)
	}
	spill.changeBytes += written
	spill.changeCount++
	return nil
}

func (spill *TransactionSpill) reader() (io.Reader, uint64, uint32, error) {
	spill.mu.Lock()
	defer spill.mu.Unlock()
	if spill.file == nil {
		return nil, 0, 0, errors.New("cdc: read closed transaction spill")
	}
	return io.NewSectionReader(spill.file, 0, int64(spill.changeBytes)),
		spill.changeBytes, spill.changeCount, nil
}

func (spill *TransactionSpill) closeAndRemove() error {
	spill.mu.Lock()
	defer spill.mu.Unlock()
	if spill.file == nil {
		err := os.Remove(spill.path)
		if errors.Is(err, os.ErrNotExist) {
			err = nil
		}
		if spill.path != "" {
			err = errors.Join(err, syncDirectory(filepath.Dir(spill.path)))
		}
		spill.path = ""
		return err
	}
	removeErr := os.Remove(spill.path)
	directoryErr := syncDirectory(filepath.Dir(spill.path))
	unlockErr := unlockSpill(spill.file)
	closeErr := spill.file.Close()
	spill.file = nil
	spill.path = ""
	return errors.Join(removeErr, directoryErr, unlockErr, closeErr)
}

func (spill *TransactionSpill) closeKeepFile() error {
	spill.mu.Lock()
	defer spill.mu.Unlock()
	if spill.file == nil {
		return nil
	}
	unlockErr := unlockSpill(spill.file)
	closeErr := spill.file.Close()
	spill.file = nil
	return errors.Join(unlockErr, closeErr)
}

// CleanupOrphanSpills removes only unlocked regular spill files created by
// this package. Files held by another active decoder/persister are skipped.
func CleanupOrphanSpills(directory string) error {
	entries, err := os.ReadDir(directory)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("cdc: read spill directory: %w", err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, spillFilePrefix) || !strings.HasSuffix(name, ".tmp") {
			continue
		}
		if entry.Type()&os.ModeSymlink != 0 || entry.IsDir() {
			continue
		}
		path := filepath.Join(directory, name)
		file, err := os.OpenFile(path, os.O_RDWR, 0)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("cdc: open orphan spill %s: %w", name, err)
		}
		if err := lockSpill(file, true); err != nil {
			_ = file.Close()
			if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
				continue
			}
			return fmt.Errorf("cdc: inspect orphan spill %s: %w", name, err)
		}
		removeErr := os.Remove(path)
		_ = unlockSpill(file)
		closeErr := file.Close()
		if removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			return fmt.Errorf("cdc: remove orphan spill %s: %w", name, removeErr)
		}
		if closeErr != nil {
			return fmt.Errorf("cdc: close orphan spill %s: %w", name, closeErr)
		}
	}
	return syncDirectory(directory)
}

func cleanupOrphanSpillsOnce(directory string) error {
	if _, loaded := cleanedSpillDirectories.LoadOrStore(directory, struct{}{}); loaded {
		return nil
	}
	if err := CleanupOrphanSpills(directory); err != nil {
		cleanedSpillDirectories.Delete(directory)
		return err
	}
	return nil
}

func lockSpill(file *os.File, nonblocking bool) error {
	operation := syscall.LOCK_EX
	if nonblocking {
		operation |= syscall.LOCK_NB
	}
	return syscall.Flock(int(file.Fd()), operation)
}

func unlockSpill(file *os.File) error {
	return syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
}
