package main

import (
	"github.com/joho/godotenv"
	"github.com/pyama86/alterguard/cmd"
)

func main() {
	// Load .env file if it exists (for local development)
	_ = godotenv.Load()

	cmd.Execute()
}
