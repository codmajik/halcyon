package halcyon

import (
	"errors"
	nats "github.com/nats-io/go-nats"
	"regexp"
	"time"
)

var ErrClientRetrySoon = errors.New("Reconnecting Retry Soon")

type Client struct {
	cl *nats.Conn
	re *regexp.Regexp
}

func (c *Client) Call(serviceName, reqUri string, data []byte) (interface{}, error) {
	rpcName := "halcyon." + serviceNameSanitize.ReplaceAllString(serviceName, "") + ".service." + reqUri

	msg, err := c.cl.Request(rpcName, data, 15*time.Second)
	if err != nil {
		return nil, err
	}

	return msg.Data, nil
}

func (c *Client) Notify(serviceName string, data []byte) error {
	if !c.cl.IsConnected() {
		if c.cl.IsReconnecting() {
			return ErrClientRetrySoon
		}
	}

	rpcName := "halcyon." + serviceNameSanitize.ReplaceAllString(serviceName, "") + ".notify"
	return c.cl.Publish(rpcName, data)
}

func (c *Client) NotifyAll(data []byte) error {
	return c.cl.Publish("halcyon.system.notify", data)
}
