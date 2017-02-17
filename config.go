package halcyon

type config struct {
	ServiceName        string   `toml:"service_name" json:"service_name"`
	NatsUri            []string `toml:"nats_uri" json:"nats_uri"`
	NotificationTopics []string `toml:"notifications" json:"notifications"`
}

type ConfigFn func(*config)

func ConfigServiceName(name string) ConfigFn {
	return func(c *config) {
		c.ServiceName = name
	}
}

func ConfigNatsUri(uri ...string) ConfigFn {
	return func(c *config) {
		c.NatsUri = append(c.NatsUri, uri...)
	}
}

func ConfigNotifyTopic(topic ...string) ConfigFn {
	return func(c *config) {
		c.NotificationTopics = append(c.NotificationTopics, topic...)
	}
}
