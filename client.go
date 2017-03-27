package halcyon

import (
	"errors"
	"fmt"
	nats "github.com/nats-io/go-nats"
	"regexp"
	"time"
)

var ErrClientRetrySoon = errors.New("Reconnecting Soon")

type Client struct {
	cl *nats.Conn
	re *regexp.Regexp
}

func (c *Client) Call(serviceName, reqUri string, data []byte) (interface{}, error) {
	rpcName := fmt.Sprintf("halcyon.rpc.%s.%s", sanitizeServiceName(serviceName), reqUri)

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

	rpcName := "halcyon.notify." + sanitizeServiceName(serviceName)
	return c.cl.Publish(rpcName, data)
}

func (c *Client) NotifyAll(data []byte) error {
	return c.cl.Publish("halcyon.notify.system", data)
}
