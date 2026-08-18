// Command screenshot rasterises the standalone preview HTML files written by
// `task preview` into PNGs that can be embedded in README.md.
//
// It is a separate module on purpose. Driving a browser needs go-rod and its
// dependency tail, and none of that belongs in arc-ui's own module graph: the
// Dockerfile's `deps` stage is cache-keyed on go.mod/go.sum alone, so a library
// that never ships in the binary would still invalidate that layer and be
// downloaded for every image build. Nothing here imports the application — it
// turns a directory of HTML files into a directory of PNGs and knows no more
// than that.
//
// The preview HTML is self-contained (the CSS is inlined and the only script tag
// has an empty src), so it renders from file:// with no server and no network.
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/go-rod/rod"
	"github.com/go-rod/rod/lib/launcher"
	"github.com/go-rod/rod/lib/proto"
)

// freezeCSS is injected before every capture. Without it the live-status pulse
// and the chart transitions are caught mid-animation, so re-running the task
// produces different pixels every time and a screenshot refresh shows up as a
// binary diff even when nothing about the UI changed.
const freezeCSS = `*, *::before, *::after {
	animation: none !important;
	transition: none !important;
	caret-color: transparent !important;
}`

func main() {
	log.SetFlags(0)
	if err := run(); err != nil {
		log.Fatalf("screenshot: %v", err)
	}
}

func run() error {
	var (
		in      = flag.String("in", "docs/screenshots", "directory holding the preview *.html files")
		out     = flag.String("out", "", "directory to write *.png into (default: same as -in)")
		width   = flag.Int("width", 1440, "viewport width in CSS pixels")
		height  = flag.Int("height", 900, "viewport height in CSS pixels; with -full this is only the starting height")
		scale   = flag.Float64("scale", 1, "device pixel ratio; 2 is retina-sharp at roughly triple the file size")
		full    = flag.Bool("full", true, "capture the whole scrollable page rather than just the viewport")
		bin     = flag.String("bin", "", "browser binary to drive (default: $ARC_UI_CHROME, else an installed Chrome/Chromium, else downloaded)")
		timeout = flag.Duration("timeout", 3*time.Minute, "overall budget for the whole run")
	)
	flag.Parse()

	if *out == "" {
		*out = *in
	}
	if *width <= 0 || *height <= 0 {
		return fmt.Errorf("-width and -height must be positive, got %dx%d", *width, *height)
	}
	if *scale <= 0 {
		return fmt.Errorf("-scale must be positive, got %v", *scale)
	}

	pages, err := filepath.Glob(filepath.Join(*in, "*.html"))
	if err != nil {
		return fmt.Errorf("glob %s: %w", *in, err)
	}
	if len(pages) == 0 {
		return fmt.Errorf("no *.html found in %s — run `task preview` first", *in)
	}
	sort.Strings(pages)

	if err := os.MkdirAll(*out, 0o750); err != nil {
		return fmt.Errorf("create %s: %w", *out, err)
	}

	browser, cleanup, err := connect(*bin)
	if err != nil {
		return err
	}
	defer cleanup()

	browser = browser.Timeout(*timeout)

	for _, src := range pages {
		dst := filepath.Join(*out, strings.TrimSuffix(filepath.Base(src), ".html")+".png")
		n, err := shoot(browser, src, dst, *width, *height, *scale, *full)
		if err != nil {
			return fmt.Errorf("%s: %w", filepath.Base(src), err)
		}
		log.Printf("wrote %s (%s)", dst, humanBytes(n))
	}
	return nil
}

// connect resolves a browser and connects to it. The lookup order is explicit
// override, then whatever is already installed, then a managed download — so a
// developer who has Chrome gets an instant run, and a bare CI box still works
// without anyone having to install a system package first.
func connect(bin string) (*rod.Browser, func(), error) {
	if bin == "" {
		bin = os.Getenv("ARC_UI_CHROME")
	}

	l := launcher.New().
		Headless(true).
		// Chromium refuses to start as root without this, which is exactly the
		// situation inside most CI containers.
		NoSandbox(true).
		Set("hide-scrollbars").
		// /dev/shm is 64 MiB by default in a container, and Chromium renders a
		// tall page straight into it before crashing on the ones that do not fit.
		Set("disable-dev-shm-usage").
		// Pin colour handling so a page rasterises identically whatever profile
		// the host monitor happens to advertise.
		Set("force-color-profile", "srgb").
		Set("disable-lcd-text")

	if bin != "" {
		// The path comes from a flag or the environment — i.e. from whoever is
		// already running this command — so there is no privilege boundary to
		// cross here. Cleaning it keeps gosec's taint analysis quiet and turns a
		// sloppy path into a tidy one in the error message.
		bin = filepath.Clean(bin)
		if _, err := os.Stat(bin); err != nil {
			return nil, nil, fmt.Errorf("browser binary %q: %w", bin, err)
		}
		l = l.Bin(bin)
	} else if found, ok := launcher.LookPath(); ok {
		l = l.Bin(found)
	}
	// Neither set: rod downloads a browser into its own cache when it launches.

	url, err := l.Launch()
	if err != nil {
		return nil, nil, fmt.Errorf("launch browser (point -bin or ARC_UI_CHROME at one): %w", err)
	}

	browser := rod.New().ControlURL(url)
	if err := browser.Connect(); err != nil {
		l.Kill()
		return nil, nil, fmt.Errorf("connect to browser: %w", err)
	}

	return browser, func() {
		_ = browser.Close()
		l.Kill()
	}, nil
}

func shoot(browser *rod.Browser, src, dst string, width, height int, scale float64, full bool) (int, error) {
	abs, err := filepath.Abs(src)
	if err != nil {
		return 0, fmt.Errorf("resolve path: %w", err)
	}

	page, err := browser.Page(proto.TargetCreateTarget{URL: "about:blank"})
	if err != nil {
		return 0, fmt.Errorf("open tab: %w", err)
	}
	defer func() { _ = page.Close() }()

	if err := page.SetViewport(&proto.EmulationSetDeviceMetricsOverride{
		Width:             width,
		Height:            height,
		DeviceScaleFactor: scale,
	}); err != nil {
		return 0, fmt.Errorf("set viewport: %w", err)
	}

	// file:// keeps this offline and serverless; the preview HTML inlines its CSS
	// precisely so that works.
	if err := page.Navigate("file://" + abs); err != nil {
		return 0, fmt.Errorf("navigate: %w", err)
	}
	if err := page.WaitLoad(); err != nil {
		return 0, fmt.Errorf("wait for load: %w", err)
	}
	if _, err := page.Eval(`() => document.fonts.ready`); err != nil {
		return 0, fmt.Errorf("wait for fonts: %w", err)
	}
	if _, err := page.Eval(`css => {
		const s = document.createElement('style');
		s.textContent = css;
		document.head.appendChild(s);
	}`, freezeCSS); err != nil {
		return 0, fmt.Errorf("freeze animations: %w", err)
	}
	if err := page.WaitStable(300 * time.Millisecond); err != nil {
		return 0, fmt.Errorf("wait for a stable frame: %w", err)
	}

	buf, err := page.Screenshot(full, &proto.PageCaptureScreenshot{
		Format:                proto.PageCaptureScreenshotFormatPng,
		CaptureBeyondViewport: full,
	})
	if err != nil {
		return 0, fmt.Errorf("capture: %w", err)
	}
	if len(buf) == 0 {
		return 0, errors.New("capture returned no bytes")
	}

	if err := os.WriteFile(dst, buf, 0o600); err != nil {
		return 0, fmt.Errorf("write %s: %w", dst, err)
	}
	return len(buf), nil
}

func humanBytes(n int) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for v := int64(n) / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGT"[exp])
}
