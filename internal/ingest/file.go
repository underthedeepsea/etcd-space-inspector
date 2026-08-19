// Package ingest validates and copies untrusted analyzer inputs.
package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// Metadata describes an immutable copied input.
type Metadata struct {
	Size    int64
	SHA256  string
	ModTime time.Time
}

// ProgressFunc receives copied and total source bytes after each buffer.
type ProgressFunc func(copied, total int64) error

// Copy validates a regular source, streams it to a private destination, and hashes it.
func Copy(ctx context.Context, source, destination string, maxBytes int64) (Metadata, error) {
	return CopyWithProgress(ctx, source, destination, maxBytes, nil)
}

// CopyWithProgress copies through a partial file and atomically replaces the destination on success.
func CopyWithProgress(ctx context.Context, source, destination string, maxBytes int64, progress ProgressFunc) (meta Metadata, err error) {
	info, err := os.Lstat(source)
	if err != nil {
		return meta, fmt.Errorf("inspect input: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return meta, fmt.Errorf("input must be a regular non-symlink file")
	}
	if maxBytes <= 0 || info.Size() > maxBytes {
		return meta, fmt.Errorf("input exceeds %d bytes", maxBytes)
	}

	in, err := os.Open(source)
	if err != nil {
		return meta, fmt.Errorf("open input: %w", err)
	}
	defer in.Close()
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return meta, fmt.Errorf("create destination directory: %w", err)
	}
	partial := destination + ".partial"
	if err := os.Remove(partial); err != nil && !os.IsNotExist(err) {
		return meta, fmt.Errorf("remove partial destination: %w", err)
	}
	out, err := os.OpenFile(partial, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return meta, fmt.Errorf("create destination: %w", err)
	}
	complete := false
	defer func() {
		if out != nil {
			closeErr := out.Close()
			if err == nil && closeErr != nil {
				err = fmt.Errorf("close destination: %w", closeErr)
			}
		}
		if !complete || err != nil {
			_ = os.Remove(partial)
		}
	}()

	hash := sha256.New()
	buffer := make([]byte, 128*1024)
	var written int64
	for {
		if err := ctx.Err(); err != nil {
			return meta, fmt.Errorf("copy input: %w", err)
		}
		read, readErr := in.Read(buffer)
		if read > 0 {
			written += int64(read)
			if written > maxBytes {
				return meta, fmt.Errorf("input exceeds %d bytes", maxBytes)
			}
			if _, err := out.Write(buffer[:read]); err != nil {
				return meta, fmt.Errorf("copy input: %w", err)
			}
			if _, err := hash.Write(buffer[:read]); err != nil {
				return meta, fmt.Errorf("hash input: %w", err)
			}
			if progress != nil {
				if err := progress(written, info.Size()); err != nil {
					return meta, fmt.Errorf("copy progress: %w", err)
				}
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return meta, fmt.Errorf("copy input: %w", readErr)
		}
	}
	if err := out.Sync(); err != nil {
		return meta, fmt.Errorf("sync destination: %w", err)
	}
	if err := out.Close(); err != nil {
		out = nil
		return meta, fmt.Errorf("close destination: %w", err)
	}
	out = nil
	if err := os.Rename(partial, destination); err != nil {
		return meta, fmt.Errorf("replace destination: %w", err)
	}
	complete = true
	return Metadata{Size: written, SHA256: hex.EncodeToString(hash.Sum(nil)), ModTime: info.ModTime()}, nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}
