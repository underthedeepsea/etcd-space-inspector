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

// Copy validates a regular source, streams it to a private destination, and hashes it.
func Copy(ctx context.Context, source, destination string, maxBytes int64) (meta Metadata, err error) {
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
	out, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return meta, fmt.Errorf("create destination: %w", err)
	}
	complete := false
	defer func() {
		closeErr := out.Close()
		if err == nil && closeErr != nil {
			err = fmt.Errorf("close destination: %w", closeErr)
		}
		if !complete || err != nil {
			_ = os.Remove(destination)
		}
	}()

	hash := sha256.New()
	if err := ctx.Err(); err != nil {
		return meta, fmt.Errorf("copy input: %w", err)
	}
	reader := &contextReader{ctx: ctx, reader: io.LimitReader(in, maxBytes+1)}
	written, err := io.CopyBuffer(io.MultiWriter(out, hash), reader, make([]byte, 128*1024))
	if err != nil {
		return meta, fmt.Errorf("copy input: %w", err)
	}
	if written > maxBytes {
		return meta, fmt.Errorf("input exceeds %d bytes", maxBytes)
	}
	if err := out.Sync(); err != nil {
		return meta, fmt.Errorf("sync destination: %w", err)
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
