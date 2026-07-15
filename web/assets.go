// Package web serves embedded analyzer UI assets.
package web

import (
	"bytes"
	"embed"
	"mime"
	"net/http"
	"path"
	"strings"
	"time"
)

//go:embed all:dist fallback/index.html
var assets embed.FS

// Handler returns an embedded static and SPA handler.
func Handler() http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet && request.Method != http.MethodHead {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		requested := strings.TrimPrefix(path.Clean(request.URL.Path), "/")
		if requested != "." && requested != "" {
			if data, err := assets.ReadFile("dist/" + requested); err == nil {
				serveAsset(writer, request, requested, data)
				return
			}
			if path.Ext(requested) != "" {
				http.NotFound(writer, request)
				return
			}
		}
		data, err := assets.ReadFile("dist/index.html")
		if err != nil {
			data, _ = assets.ReadFile("fallback/index.html")
		}
		serveAsset(writer, request, "index.html", data)
	})
}

func serveAsset(writer http.ResponseWriter, request *http.Request, name string, data []byte) {
	if contentType := mime.TypeByExtension(path.Ext(name)); contentType != "" {
		writer.Header().Set("Content-Type", contentType)
	}
	if request.Method == http.MethodHead {
		writer.WriteHeader(http.StatusOK)
		return
	}
	http.ServeContent(writer, request, name, time.Time{}, bytes.NewReader(data))
}
