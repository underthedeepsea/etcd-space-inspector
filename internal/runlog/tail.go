package runlog

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
)

// Tail returns at most lines from the end of path while reading at most maxBytes.
func Tail(path string, lines int, maxBytes int64) ([]string, error) {
	if lines <= 0 || maxBytes <= 0 {
		return []string{}, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open log tail: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat log tail: %w", err)
	}
	offset := info.Size() - maxBytes
	if offset < 0 {
		offset = 0
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek log tail: %w", err)
	}
	data, err := io.ReadAll(io.LimitReader(file, maxBytes))
	if err != nil {
		return nil, fmt.Errorf("read log tail: %w", err)
	}
	if offset > 0 {
		if index := bytes.IndexByte(data, '\n'); index >= 0 {
			data = data[index+1:]
		} else {
			data = nil
		}
	}
	data = bytes.TrimRight(data, "\r\n")
	if len(data) == 0 {
		return []string{}, nil
	}
	result := strings.Split(string(data), "\n")
	if len(result) > lines {
		result = result[len(result)-lines:]
	}
	return result, nil
}
