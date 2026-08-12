package auditanalysis

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/netip"
	"os"
	"strings"
	"time"
)

const (
	maxLineBytes          = 8 << 20
	maxExpandedInputBytes = int64(100) << 30
)

// ErrExpandedInputTooLarge prevents decompression bombs and runaway imports.
var ErrExpandedInputTooLarge = errors.New("audit expanded input exceeds 100 GiB")

type auditEvent struct {
	AuditID         string     `json:"auditID"`
	Stage           string     `json:"stage"`
	StageTimestamp  *time.Time `json:"stageTimestamp"`
	RequestReceived *time.Time `json:"requestReceivedTimestamp"`
	Verb            string     `json:"verb"`
	User            struct {
		Username string `json:"username"`
	} `json:"user"`
	UserAgent string   `json:"userAgent"`
	SourceIPs []string `json:"sourceIPs"`
	ObjectRef struct {
		APIVersion  string `json:"apiVersion"`
		Resource    string `json:"resource"`
		Subresource string `json:"subresource"`
		Namespace   string `json:"namespace"`
		Name        string `json:"name"`
	} `json:"objectRef"`
	ResponseStatus struct {
		Code int `json:"code"`
	} `json:"responseStatus"`
	RequestObject  json.RawMessage `json:"requestObject"`
	ResponseObject json.RawMessage `json:"responseObject"`
}

// ParseFile streams JSON-lines Audit data, detecting gzip from its magic bytes.
func ParseFile(ctx context.Context, path string, sink EventSink) (Summary, error) {
	if sink == nil {
		sink = func(context.Context, Event) error { return nil }
	}
	if err := ctx.Err(); err != nil {
		return Summary{}, err
	}
	file, err := os.Open(path)
	if err != nil {
		return Summary{}, err
	}
	defer file.Close()

	header := make([]byte, 2)
	n, readErr := io.ReadFull(file, header)
	if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
		return Summary{}, readErr
	}
	reader := io.Reader(io.MultiReader(bytes.NewReader(header[:n]), file))
	if n == 2 && header[0] == 0x1f && header[1] == 0x8b {
		compressed, err := gzip.NewReader(reader)
		if err != nil {
			return Summary{ParseErrors: 1}, nil
		}
		defer compressed.Close()
		reader = compressed
	}
	return parseReaderLimited(ctx, reader, sink, maxExpandedInputBytes)
}

func parseReader(ctx context.Context, reader io.Reader, sink EventSink) (Summary, error) {
	return parseReaderLimited(ctx, reader, sink, maxExpandedInputBytes)
}

func parseReaderLimited(ctx context.Context, reader io.Reader, sink EventSink, maxBytes int64) (Summary, error) {
	if sink == nil {
		sink = func(context.Context, Event) error { return nil }
	}
	limited := &io.LimitedReader{R: reader, N: maxBytes + 1}
	buffered := bufio.NewReaderSize(limited, 64*1024)
	var summary Summary
	for {
		if err := ctx.Err(); err != nil {
			return summary, err
		}
		line, readErr, overlong, err := readBoundedLine(ctx, buffered)
		if err != nil {
			return summary, err
		}
		if len(line) == 0 && readErr == io.EOF {
			break
		}
		summary.TotalLines++
		lineNumber := summary.TotalLines
		if overlong {
			summary.ParseErrors++
			summary.UnknownLines++
			if readErr == io.EOF {
				break
			}
			continue
		}
		var raw auditEvent
		if err := json.Unmarshal(bytes.TrimSpace(line), &raw); err != nil {
			summary.ParseErrors++
			summary.UnknownLines++
		} else {
			event := normalize(raw)
			event.LineNumber = lineNumber
			if raw.AuditID == "" {
				event.AuditIDHash = hashBytes(bytes.TrimSpace(line))
			}
			summary.ValidEvents++
			if IsWriteVerb(event.Verb) {
				summary.WriteEvents++
			}
			updateObservedRange(&summary, event.ObservedAt)
			if err := sink(ctx, event); err != nil {
				return summary, err
			}
		}
		if readErr == io.EOF {
			break
		}
	}
	if limited.N == 0 {
		return summary, ErrExpandedInputTooLarge
	}
	return summary, nil
}

func readBoundedLine(ctx context.Context, reader *bufio.Reader) ([]byte, error, bool, error) {
	var line []byte
	overlong := false
	for {
		if err := ctx.Err(); err != nil {
			return line, nil, overlong, err
		}
		chunk, readErr := reader.ReadSlice('\n')
		if !overlong {
			remaining := maxLineBytes + 1 - len(line)
			if remaining <= 0 || len(chunk) > remaining {
				if remaining > 0 {
					line = append(line, chunk[:remaining]...)
				}
				overlong = true
			} else {
				line = append(line, chunk...)
				if len(line) > maxLineBytes {
					overlong = true
				}
			}
		}
		if readErr == bufio.ErrBufferFull {
			continue
		}
		return line, readErr, overlong, nil
	}
}

func normalize(raw auditEvent) Event {
	group := strings.SplitN(raw.ObjectRef.APIVersion, "/", 2)[0]
	if !strings.Contains(raw.ObjectRef.APIVersion, "/") {
		group = ""
	}
	keyHash, displayName := ObjectKeyHash(group, raw.ObjectRef.Resource, raw.ObjectRef.Namespace, raw.ObjectRef.Name)
	observedAt := raw.StageTimestamp
	if observedAt == nil {
		observedAt = raw.RequestReceived
	}
	userAgent := strings.Fields(raw.UserAgent)
	firstUserAgent := ""
	if len(userAgent) > 0 {
		firstUserAgent = userAgent[0]
	}
	sourceNetwork, sourceHash := "", ""
	if len(raw.SourceIPs) > 0 {
		sourceNetwork, sourceHash = normalizeSourceIP(raw.SourceIPs[0])
	}
	username := raw.User.Username
	if username == "" {
		username = "unknown"
	}
	return Event{
		AuditIDHash: hashString(raw.AuditID), ObservedAt: observedAt, Stage: raw.Stage,
		StageRank: stageRank(raw.Stage), Verb: strings.ToLower(raw.Verb),
		Username: username, UsernameHash: hashString(raw.User.Username),
		UserAgent: firstUserAgent, UserAgentHash: hashString(raw.UserAgent),
		SourceNetwork: sourceNetwork, SourceIPHash: sourceHash,
		APIGroup: group, Resource: raw.ObjectRef.Resource, Subresource: raw.ObjectRef.Subresource,
		Namespace: raw.ObjectRef.Namespace, ObjectName: safeObjectName(raw.ObjectRef.Resource, raw.ObjectRef.Name),
		DisplayName: displayName, ObjectKeyHash: keyHash, ResponseCode: raw.ResponseStatus.Code,
		RequestObjectBytes: int64(len(raw.RequestObject)), ResponseObjectBytes: int64(len(raw.ResponseObject)),
		ParseStatus: "parsed",
	}
}

// IsWriteVerb reports whether a normalized verb may change persisted objects.
func IsWriteVerb(value string) bool {
	switch strings.ToLower(value) {
	case "create", "update", "patch", "delete", "deletecollection":
		return true
	default:
		return false
	}
}

// IsVerb reports whether value is in the fixed Audit verb allow-list.
func IsVerb(value string) bool {
	switch strings.ToLower(value) {
	case "get", "list", "watch", "create", "update", "patch", "delete", "deletecollection", "proxy", "connect":
		return true
	default:
		return false
	}
}

// ObjectKeyHash recreates the etcd registry key identity used by snapshot analysis.
func ObjectKeyHash(apiGroup, resource, namespace, name string) (string, string) {
	if resource == "" || name == "" {
		return "", name
	}
	parts := []string{"", "registry"}
	switch {
	case resource == "services":
		parts = append(parts, "services", "specs")
	case resource == "endpoints":
		parts = append(parts, "services", "endpoints")
	case apiGroup != "" && !isBuiltInResource(resource):
		parts = append(parts, apiGroup, resource)
	default:
		parts = append(parts, resource)
	}
	if namespace != "" {
		parts = append(parts, namespace)
	}
	if name != "" {
		parts = append(parts, name)
	}
	hash := hashString(strings.Join(parts, "/"))
	displayName := name
	if isSensitiveResource(resource) && name != "" {
		displayName = "redacted:" + hash[:12]
	}
	return hash, displayName
}

func isBuiltInResource(resource string) bool {
	switch resource {
	case "deployments", "daemonsets", "statefulsets", "replicasets", "jobs", "cronjobs", "leases", "ingresses", "networkpolicies", "storageclasses", "csinodes":
		return true
	default:
		return false
	}
}

func isSensitiveResource(resource string) bool {
	return resource == "secrets" || resource == "serviceaccounts" || resource == "certificatesigningrequests"
}

func normalizeSourceIP(value string) (string, string) {
	address, err := netip.ParseAddr(value)
	if err != nil {
		return "unknown", hashString(value)
	}
	bits := 64
	if address.Is4() {
		bits = 24
	}
	return netip.PrefixFrom(address, bits).Masked().String(), hashString(value)
}

func safeObjectName(resource, name string) string {
	if isSensitiveResource(resource) {
		return ""
	}
	return name
}

func stageRank(stage string) int {
	switch stage {
	case "ResponseComplete":
		return 4
	case "ResponseStarted":
		return 3
	case "Panic":
		return 2
	case "RequestReceived":
		return 1
	default:
		return 0
	}
}

func hashString(value string) string {
	return hashBytes([]byte(value))
}

func hashBytes(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func updateObservedRange(summary *Summary, observedAt *time.Time) {
	if observedAt == nil {
		return
	}
	if summary.FirstObservedAt == nil || observedAt.Before(*summary.FirstObservedAt) {
		copy := *observedAt
		summary.FirstObservedAt = &copy
	}
	if summary.LastObservedAt == nil || observedAt.After(*summary.LastObservedAt) {
		copy := *observedAt
		summary.LastObservedAt = &copy
	}
}
