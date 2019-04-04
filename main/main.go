package main

import (
	"os"

	"github.com/hashicorp/go-hclog"
	"github.com/lyraproj/identity/identity"
)

func init() {
	// Configuring hclog like this allows Lyra to handle log levels automatically
	hclog.DefaultOptions = &hclog.LoggerOptions{
		Name:            "Identity",
		Level:           hclog.LevelFromString(os.Getenv("LYRA_LOG_LEVEL")),
		JSONFormat:      true,
		IncludeLocation: false,
		Output:          os.Stderr,
	}
}

func main() {
	identity.Start("identity.db")
}
