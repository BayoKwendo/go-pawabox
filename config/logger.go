package config

import "github.com/sirupsen/logrus"

// NewLogger sets up global logger config and returns configured logger.
func NewLogger() *logrus.Logger {
	logger := logrus.New()
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})
	logger.SetLevel(logrus.InfoLevel)
	// you can add hooks to write to files or external systems
	return logger
}
