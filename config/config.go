/*
Copyright 2017 Crunchy Data Solutions, Inc.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"

	"github.com/crunchydata/crunchy-proxy/common"
	"github.com/crunchydata/crunchy-proxy/util/log"
)

var c Config

func init() {
	viper.SetConfigType("yaml")
	viper.SetConfigName("config")
	viper.AddConfigPath("/etc/crunchy-proxy")
	viper.AddConfigPath(".")
}

func GetConfig() Config {
	return c
}

func GetNodes() map[string]common.Node {
	return c.Nodes
}

func GetDefaultBackend() string {
	return c.DefaultBackend
}

func GetPickSessionLevel() string {
	if c.PickSessionLevel == "" {
		return "none"
	}
	return c.PickSessionLevel
}

func GetConnectionTTL() int {
	if c.ConnectionTTL == 0 {
		return 90000
	}
	return c.ConnectionTTL
}

func GetQueryRoutes() map[string]common.QueryRoute {
	return c.Routes
}

func GetProxyConfig() ProxyConfig {
	return c.Server.Proxy
}

func GetAdminConfig() AdminConfig {
	return c.Server.Admin
}

func GetPoolCapacity() int {
	return c.Pool.Capacity
}

func GetPoolEnable() bool {
	return c.Pool.Enable
}

func GetForceRouting() bool {
	return c.Pool.ForceRouting
}

func GetCredentials(role string) common.Credentials {
	return c.Credentials[role]
}

func HasClient(userName string, authMethod int32) bool {
	if c.Clients == "any" {
		return true
	}

	allowedList := strings.Split(c.Clients, ",")
	for _, allowed := range allowedList {
		if allowed == userName {
			return true
		}
		if allowed == fmt.Sprintf("auth=%d", authMethod) {
			return true
		}
		if strings.HasPrefix(allowed, "auth=") && authMethod == -1 {
			return true
		}
	}
	return false
}

func GetHealthCheckConfig() common.HealthCheckConfig {
	return c.HealthCheck
}

func Get(key string) interface{} {
	return viper.Get(key)
}

func GetBool(key string) bool {
	return viper.GetBool(key)
}

func GetInt(key string) int {
	return viper.GetInt(key)
}

func GetString(key string) string {
	return viper.GetString(key)
}

func GetStringMapString(key string) map[string]string {
	return viper.GetStringMapString(key)
}

func GetStringMap(key string) map[string]interface{} {
	return viper.GetStringMap(key)
}

func GetStringSlice(key string) []string {
	return viper.GetStringSlice(key)
}

func IsSet(key string) bool {
	return viper.IsSet(key)
}

func Set(key string, value interface{}) {
	viper.Set(key, value)
}

type ProxyConfig struct {
	HostPort string `mapstructure:"hostport"`
}

type AdminConfig struct {
	HostPort string `mapstructure:"hostport"`
}

type ServerConfig struct {
	Admin AdminConfig `mapstructure:"admin"`
	Proxy ProxyConfig `mapstructure:"proxy"`
}

type PoolConfig struct {
	Capacity     int  `mapstructure:"capacity"`
	Enable       bool `mapstructure:"enable"`
	ForceRouting bool `mapstructure:"forceRouting"`
}

type Adapter struct {
	AdapterType string                 `mapstructure:"adaptertype"`
	Metadata    map[string]interface{} `mapstructure:"metadata"`
}

type Config struct {
	//Nodes       map[string]common.Node `mapstructure:"nodes"`
	Server           ServerConfig                  `mapstructure:"server"`
	Pool             PoolConfig                    `mapstructure:"pool"`
	Nodes            map[string]common.Node        `mapstructure:"nodes"`
	Credentials      map[string]common.Credentials `mapstructure:"credentials"`
	HealthCheck      common.HealthCheckConfig      `mapstructure:"healthcheck"`
	Routes           map[string]common.QueryRoute  `mapstructure:"routes"`
	Clients          string                        `mapstructure:"clients"`
	DefaultBackend   string                        `mapstructure:"default-backend"`
	PickSessionLevel string                        `mapstructure:"pick-session-level"`
	ConnectionTTL    int                           `mapstructure:"connection-ttl"`
}

func SetConfigPath(path string) {
	viper.SetConfigFile(path)
}

func ReadConfig() {
	err := viper.ReadInConfig()
	log.Debugf("Using configuration file: %s", viper.ConfigFileUsed())

	if err != nil {
		log.Fatal(err.Error())
	}

	err = viper.Unmarshal(&c)

	if err != nil {
		log.Errorf("Error unmarshaling configuration file: %s", viper.ConfigFileUsed())
		log.Fatalf(err.Error())
	}
}
