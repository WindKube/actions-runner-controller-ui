package web

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"net/http"
	"path"
	"strings"
	"time"
)

// staticFS holds the browser assets: the Tailwind bundle and the Datastar
// runtime.
//
// The `all:` prefix matters. Without it, embed silently skips files beginning
// with "." or "_", which would drop the committed .gitkeep and — on a tree
// where Tailwind has not run — leave the directory empty. An empty directory is
// a compile-time error for go:embed, so the .gitkeep is what lets a fresh clone
// build before `task gen` has ever run.
//
//go:embed all:static
var staticFS embed.FS

// AssetPrefix is where assets are mounted.
const AssetPrefix = "/static/"

// Assets serves the embedded browser assets under content-hashed names.
//
// Hashing is not an optimisation here, it is the only workable cache strategy.
// Files read out of an embed.FS report a zero modification time, so
// http.ServeContent emits neither Last-Modified nor ETag and every browser
// revalidates on every load. Putting the content hash in the filename lets the
// response be marked immutable, and guarantees a deploy that changes the CSS
// changes its URL.
type Assets struct {
	// byPath maps a request path to the bytes to serve.
	byPath map[string]asset
	// urls maps a logical name ("app.css") to its hashed request path.
	urls map[string]string
}

type asset struct {
	body        []byte
	contentType string
	// immutable is true for the hashed path and false for the plain one, so a
	// hand-typed /static/app.css is never cached for a year.
	immutable bool
	etag      string
}

// NewAssets indexes the embedded files.
func NewAssets() (*Assets, error) {
	a := &Assets{
		byPath: make(map[string]asset, 8),
		urls:   make(map[string]string, 4),
	}

	entries, err := fs.ReadDir(staticFS, "static")
	if err != nil {
		return nil, fmt.Errorf("read embedded assets: %w", err)
	}

	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || strings.HasPrefix(name, ".") {
			continue
		}

		body, err := staticFS.ReadFile(path.Join("static", name))
		if err != nil {
			return nil, fmt.Errorf("read embedded asset %s: %w", name, err)
		}

		sum := sha256.Sum256(body)
		hash := hex.EncodeToString(sum[:])[:12]
		ext := path.Ext(name)
		hashed := strings.TrimSuffix(name, ext) + "." + hash + ext

		item := asset{
			body:        body,
			contentType: contentType(ext),
			etag:        `"` + hash + `"`,
		}

		a.urls[name] = AssetPrefix + hashed
		a.byPath[name] = item

		item.immutable = true
		a.byPath[hashed] = item
	}

	return a, nil
}

// URL returns the hashed URL for a logical asset name. An unknown name falls
// back to the unhashed path rather than an empty string, so a missing asset
// shows up as a 404 in the browser's network tab instead of as a page with a
// mysteriously absent stylesheet link.
func (a *Assets) URL(name string) string {
	if u, ok := a.urls[name]; ok {
		return u
	}
	return AssetPrefix + name
}

// CSS is the stylesheet URL.
func (a *Assets) CSS() string { return a.URL("app.css") }

// JS is the Datastar runtime URL.
func (a *Assets) JS() string { return a.URL("datastar.js") }

// ServeHTTP serves one asset. It expects to be mounted at AssetPrefix with the
// prefix already stripped.
func (a *Assets) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/")
	item, ok := a.byPath[name]
	if !ok {
		http.NotFound(w, r)
		return
	}

	// No hand-rolled If-None-Match branch here. Writing 304 before these
	// headers are set sends a 304 carrying neither ETag nor Cache-Control,
	// which stops the browser extending the freshness lifetime and makes it
	// revalidate the hashed asset on every load — exactly what the immutable
	// strategy above exists to avoid. http.ServeContent evaluates the
	// conditional itself, provided ETag is set before the call, so setting the
	// headers first is both correct and less code.
	w.Header().Set("Content-Type", item.contentType)
	w.Header().Set("ETag", item.etag)
	if item.immutable {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	} else {
		w.Header().Set("Cache-Control", "public, max-age=300")
	}
	// A zero modtime tells ServeContent to skip Last-Modified entirely, which
	// is what we want: the ETag above is the whole caching story.
	http.ServeContent(w, r, name, time.Time{}, bytes.NewReader(item.body))
}

func contentType(ext string) string {
	switch ext {
	case ".css":
		return "text/css; charset=utf-8"
	case ".js":
		return "text/javascript; charset=utf-8"
	case ".svg":
		return "image/svg+xml"
	case ".ico":
		return "image/x-icon"
	case ".woff2":
		return "font/woff2"
	}
	return "application/octet-stream"
}
