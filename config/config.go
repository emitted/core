package config

import (
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"gopkg.in/yaml.v2"
)

// Service ...
type Service struct {
	Host string `yaml:"host"`
	Port string `yaml:"host"`
}

// Config ...
type Config struct {
	Server struct {
		Host string `yaml:"host"`
		Port string `yaml:"port"`
	} `yaml:"server"`
	Redis struct {
		Host string `yaml:"host"`
		Port string `yaml:"port"`
	} `yaml:"redis"`
	Services map[string]Service `yaml:"services"`
}

// GetConfig ...
func GetConfig() *Config {
	_, dirname, _, _ := runtime.Caller(0)

	filepath := filepath.Dir(dirname) + "/../config.yml"
	filepath = strings.Replace(filepath, "/", "\\", -1)

	f, err := os.Open(filepath)
	defer f.Close()
	if err != nil {
		log.Fatal(err)
	}

	var cfg Config
	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(&cfg)
	if err != nil {
		log.Fatal(err)
	}

	return &cfg
}
