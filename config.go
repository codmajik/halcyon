package halcyon

import (
	nats "github.com/nats-io/go-nats"
	"regexp"
	"strings"
)

type config struct {
	ServiceName        string   `toml:"service_name" json:"service_name"`
	NatsURI            []string `toml:"nats_uri" json:"nats_uri"`
	NatsConn           *nats.Conn
	NotificationTopics []string `toml:"notifications" json:"notifications"`
}

type ConfigFn func(*config)

// ConfigServiceName set name of service
func ConfigServiceName(name string) ConfigFn {
	return func(c *config) {
		c.ServiceName = name
	}
}

// ConfigNatsConn set current connection
func ConfigNatsConn(conn *nats.Conn) ConfigFn {
	return func(c *config) {
		c.NatsConn = conn
	}
}

// ConfigNatsURI url to nats server
func ConfigNatsURI(uri ...string) ConfigFn {
	return func(c *config) {
		c.NatsURI = append(c.NatsURI, uri...)
	}
}

// ConfigNotifyTopic service to be notified about
func ConfigNotifyTopic(topic ...string) ConfigFn {
	return func(c *config) {
		c.NotificationTopics = append(c.NotificationTopics, topic...)
	}
}

var serviceNameSanitize, _ = regexp.Compile("[^a-zA-Z0-9]")

func sanitizeServiceName(name string) string {
	return strings.ToLower(serviceNameSanitize.ReplaceAllString(name, ""))
}
