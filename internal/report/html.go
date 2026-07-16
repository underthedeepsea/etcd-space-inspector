// Package report renders standalone, Value-free analysis summaries.
package report

import (
	"context"
	"fmt"
	"html/template"
	"io"
	"os"
	"path/filepath"

	backend "etcd-analyzer/internal/backend/bbolt"
	"etcd-analyzer/internal/mvcc"
	"etcd-analyzer/internal/task"
)

// Summary is the strict data boundary available to the HTML renderer.
type Summary struct {
	Task              task.Task
	Physical          backend.Summary
	MVCC              mvcc.Summary
	TopCurrentKeys    []mvcc.KeyRecord
	TopHistoricalKeys []mvcc.KeyRecord
	TopPrefixes       []mvcc.PrefixStat
}

// WriteFile atomically replaces a private report file.
func WriteFile(ctx context.Context, path string, summary Summary) (err error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create report directory: %w", err)
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".report-*.html")
	if err != nil {
		return fmt.Errorf("create temporary report: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = temporary.Close()
		if err != nil {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err = temporary.Chmod(0o600); err != nil {
		return fmt.Errorf("secure temporary report: %w", err)
	}
	if err = WriteHTML(ctx, temporary, summary); err != nil {
		return err
	}
	if err = temporary.Sync(); err != nil {
		return fmt.Errorf("sync report: %w", err)
	}
	if err = temporary.Close(); err != nil {
		return fmt.Errorf("close report: %w", err)
	}
	if err = os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("replace report: %w", err)
	}
	return nil
}

// WriteHTML writes an escaped, standalone report without JavaScript.
func WriteHTML(ctx context.Context, writer io.Writer, summary Summary) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := summaryTemplate.Execute(writer, summary); err != nil {
		return fmt.Errorf("render HTML report: %w", err)
	}
	return ctx.Err()
}

var summaryTemplate = template.Must(template.New("summary").Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>ETCD DBSize Analyzer — {{.Task.Name}}</title>
<style>
:root{font-family:system-ui,sans-serif;color:#17221d;background:#eef2ee}body{max-width:1100px;margin:0 auto;padding:32px}header,section{background:#fff;border:1px solid #d5ddd7;border-radius:10px;padding:20px;margin-bottom:16px}h1{margin:0 0 8px}.meta{color:#526158;font-family:ui-monospace,monospace;overflow-wrap:anywhere}.metrics{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:10px}.metric{border:1px solid #dce4de;border-radius:8px;padding:12px}.metric span{display:block;color:#66756c;font-size:13px}.metric strong{font-size:20px}table{width:100%;border-collapse:collapse}th,td{text-align:left;padding:9px;border-top:1px solid #e1e7e2}th{color:#526158;font-size:12px}.warning{padding:12px;background:#fff4d6;border-radius:8px}@media print{body{background:#fff;padding:0}header,section{break-inside:avoid}}
</style></head><body>
<header><h1>ETCD DBSize Analyzer</h1><p>{{.Task.Name}}</p><p class="meta">SHA-256: {{.Task.SourceSHA256}}</p></header>
<section><h2>Physical space</h2><div class="metrics">
<div class="metric"><span>File bytes</span><strong>{{.Physical.PhysicalFileSize}}</strong></div>
<div class="metric"><span>In-use bytes</span><strong>{{.Physical.InUsePageBytes}}</strong></div>
<div class="metric"><span>Free bytes</span><strong>{{.Physical.FreePageBytes}}</strong></div>
<div class="metric"><span>Pages</span><strong>{{.Physical.PageCount}}</strong></div></div></section>
<section><h2>MVCC</h2>{{if .MVCC.SemanticAvailable}}<div class="metrics">
<div class="metric"><span>Current keys</span><strong>{{.MVCC.CurrentKeyCount}}</strong></div>
<div class="metric"><span>Current stored bytes</span><strong>{{.MVCC.CurrentStoredBytes}}</strong></div>
<div class="metric"><span>Historical versions</span><strong>{{.MVCC.HistoricalVersions}}</strong></div>
<div class="metric"><span>Historical bytes</span><strong>{{.MVCC.HistoricalBytes}}</strong></div>
<div class="metric"><span>Tombstones</span><strong>{{.MVCC.TombstoneCount}}</strong></div></div>
{{if .MVCC.DecodeErrors}}<p class="warning">Decode warnings: {{.MVCC.DecodeErrors}}</p>{{end}}
{{else}}<p class="warning">Semantic MVCC analysis was skipped because the source was not confirmed as etcd 3.4.x. Physical bbolt results remain available.</p>{{end}}</section>
{{if .TopPrefixes}}<section><h2>Top prefixes</h2><table><thead><tr><th>Prefix</th><th>Current keys</th><th>Historical bytes</th><th>Tombstones</th></tr></thead><tbody>{{range .TopPrefixes}}<tr><td>{{.Prefix}}</td><td>{{.CurrentKeyCount}}</td><td>{{.HistoricalBytes}}</td><td>{{.TombstoneCount}}</td></tr>{{end}}</tbody></table></section>{{end}}
{{if .TopCurrentKeys}}<section><h2>Top current-size keys</h2><table><thead><tr><th>Key</th><th>Current bytes</th><th>Revisions</th></tr></thead><tbody>{{range .TopCurrentKeys}}<tr><td>{{.KeyText}}</td><td>{{.CurrentStoredBytes}}</td><td>{{.RevisionCount}}</td></tr>{{end}}</tbody></table></section>{{end}}
{{if .TopHistoricalKeys}}<section><h2>Top historical-size keys</h2><table><thead><tr><th>Key</th><th>Historical bytes</th><th>Revisions</th><th>Tombstones</th></tr></thead><tbody>{{range .TopHistoricalKeys}}<tr><td>{{.KeyText}}</td><td>{{.HistoricalBytes}}</td><td>{{.RevisionCount}}</td><td>{{.TombstoneCount}}</td></tr>{{end}}</tbody></table></section>{{end}}
</body></html>`))
