// Package migrations embeds the task database schema.
package migrations

import "embed"

// Files contains every numbered SQL migration.
//
//go:embed *.sql
var Files embed.FS
