package runlog

import (
	"errors"
	"os"
	"regexp"
	"strings"
	"unicode/utf8"

	"etcd-analyzer/internal/apperr"
)

const maxSafeCauseBytes = 2 << 10

var (
	windowsPath = regexp.MustCompile(`(?i)\b[a-z]:\\[^\s:]*`)
	uncPath     = regexp.MustCompile(`\\\\[^\s]+`)
	unixPath    = regexp.MustCompile(`/[^\s:]+`)
)

// SafeCause returns a single-line diagnostic that omits filesystem paths.
func SafeCause(err error) string {
	if err == nil {
		return ""
	}
	return truncateSafeCause(safeCause(err))
}

func safeCause(err error) string {
	var coded *apperr.Error
	if errors.As(err, &coded) {
		return cleanSafeCause(strings.TrimSpace(coded.Code + " " + coded.Message))
	}
	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		return cleanSafeCause(strings.TrimSpace(pathErr.Op + " " + safeCause(pathErr.Err)))
	}
	var linkErr *os.LinkError
	if errors.As(err, &linkErr) {
		return cleanSafeCause(strings.TrimSpace(linkErr.Op + " " + safeCause(linkErr.Err)))
	}
	return cleanSafeCause(err.Error())
}

func cleanSafeCause(value string) string {
	value = strings.NewReplacer("\r", " ", "\n", " ", "\t", " ").Replace(value)
	value = windowsPath.ReplaceAllString(value, "[path]")
	value = uncPath.ReplaceAllString(value, "[path]")
	return unixPath.ReplaceAllString(value, "[path]")
}

func truncateSafeCause(value string) string {
	if len(value) <= maxSafeCauseBytes {
		return value
	}
	value = value[:maxSafeCauseBytes]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}
