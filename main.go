package main

import (
	"fmt"
	"orba/batcher"
	"orba/runner"
	"os"

	"github.com/spf13/cobra"
)

var root = &cobra.Command{
	Use:   "orba",
	Short: "simple tool to organize your backfilling need",
}

func main() {
	root.AddCommand(batcher.Init())
	root.AddCommand(runner.Init())
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
