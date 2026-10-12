package main

import (
	"os"

	"github.com/kevinmingtarja/k8s-time-travel/internal/cli"
)

func main() {
	os.Exit(cli.Execute())
}
