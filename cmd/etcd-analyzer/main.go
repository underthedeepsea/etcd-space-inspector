package main

import (
	"fmt"
	"io"
	"os"

	"etcd-analyzer/internal/version"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: etcd-analyzer <version|analyze|server>")
		return 2
	}

	switch args[0] {
	case "version":
		fmt.Fprintln(stdout, version.Value)
		return 0
	case "analyze", "server":
		fmt.Fprintf(stderr, "%s is not implemented yet\n", args[0])
		return 1
	default:
		fmt.Fprintf(stderr, "unknown command %q\n", args[0])
		return 2
	}
}
