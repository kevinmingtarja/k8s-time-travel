package main

import (
	"fmt"
	"os"

	"github.com/kevinmingtarja/kubectl-timeline/internal/cli"
)

// Set by the release build (see .goreleaser.yaml).
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	os.Exit(cli.Execute(fmt.Sprintf("%s (commit %s, built %s)", version, commit, date)))
}
