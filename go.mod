module github.com/WindKube/actions-runners-controller-ui

// Minor-only on purpose. A patch-pinned directive (`go 1.26.5`) makes the
// Dockerfile's `golang:1.26` base image download a second toolchain whenever the
// image lags the pin, which costs a minute of build time for nothing. Bump the
// minor when the code needs a newer language feature, not to chase releases.
go 1.26
