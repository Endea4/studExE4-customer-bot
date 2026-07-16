package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

// LoadConfig loads the .env file if it exists
func LoadConfig() {
	err := godotenv.Load()
	if err != nil {
		log.Println("No .env file found, using system environment variables")
	}
}

// GetEnv retrieves a string from the environment, returning a default if not found
func GetEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
