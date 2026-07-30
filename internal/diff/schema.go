package diff

import _ "embed"

// Schema is the complete private comparison database schema.
//
//go:embed schema.sql
var Schema string
