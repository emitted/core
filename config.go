package main

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
	Server *ServerConfig `yaml:"server"`

	Broker *BrokerConfig `yaml:"broker"`

	Services map[string]Service `yaml:"services"`
}

// BrokerConfig ...
type BrokerConfig struct {
	Host           string `yaml:"host"`
	Port           string `yaml:"port"`
	DB             int    `yaml:"db"`
	PubSubWorkers  int    `yaml:"workers"`
	ReadTimeout    uint8  `yaml:"read_timeout"`
	WriteTimeout   uint8  `yaml:"write_timeout"`
	ConnectTimeout uint8  `yaml:"connect_timeout"`
}

// ServerConfig ...
type ServerConfig struct {
	Host    string `yaml:"host"`
	Port    string `yaml:"port"`
	Workers int    `yaml:"workers"`
}

// GetConfig ...
func GetConfig() *Config {
	_, dirname, _, _ := runtime.Caller(0)

	filepath := filepath.Dir(dirname) + "/config.yml"
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
