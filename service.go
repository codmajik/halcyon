package halcyon

import (
	"errors"
	"fmt"
	"github.com/Sirupsen/logrus"
	sr "github.com/codmajik/servicerouter"
	nats "github.com/nats-io/go-nats"
	"github.com/nats-io/nuid"
	"golang.org/x/net/context"
	"strings"
	"time"
)

var ErrBadInst = errors.New("Halcyon: not instantiated")

type Handler interface {
	Init(*sr.Router, *Client) error
	Notify(context.Context, string, interface{})
}

type Halcyon struct {
	conn        *nats.Conn
	done        chan bool
	conf        *config
	serviceName string
	service     Handler
	router      *sr.Router
	cl          *Client
	log         *logrus.Entry
}

func NewHalcyon(srv Handler, opts ...ConfigFn) *Halcyon {

	cfg := &config{
		NatsURI:            make([]string, 0),
		NotificationTopics: []string{"system"},
		ServiceName:        "unknamed-" + nuid.Next(),
	}

	for _, opt := range opts {
		opt(cfg)
	}

	return &Halcyon{
		conf:        cfg,
		serviceName: sanitizeServiceName(cfg.ServiceName),
		service:     srv,
		done:        make(chan bool),
		cl:          &Client{},
		log: logrus.WithFields(logrus.Fields{
			"service": cfg.ServiceName,
			"ctx":     "halcyon",
		}),
	}
}

func (h *Halcyon) connect() error {

	if h == nil {
		return ErrBadInst
	}

	clName := "halcyon.service." + h.serviceName + "-" + nuid.Next()
	timeout := int64(0)
	for {
		if h.conn != nil {
			break
		}

		time.Sleep(time.Duration(timeout) * time.Second)
		conn, err := nats.Connect(
			strings.Join(h.conf.NatsURI, ","),
			nats.Name(clName),
			nats.MaxReconnects(-1),
		)
		if err != nil {
			h.log.WithError(err).
				Errorf("unable to connect to %s as %s", h.conf.NatsURI, clName)

			if timeout*2 > 20 {
				timeout = 20
			} else {
				timeout = (timeout * 2) + 1
			}

			h.log.Info("retrying in ", timeout)
			continue
		}

		h.conn = conn
	}

	h.log.Infof("connected to server [id:%s, uri:%s]", h.conn.ConnectedServerId(), h.conn.ConnectedUrl())
	h.conn.SetDisconnectHandler(func(c *nats.Conn) {
		h.log.Error("disconnected from NATS cluster...trying again!!!")
	})

	h.conn.SetReconnectHandler(func(c *nats.Conn) {
		h.log.Info("reconnected NATS cluster")
	})
	return nil
}

// NatsConn returns internal nats connection
func (h *Halcyon) NatsConn() *nats.Conn {
	return h.conn
}

func (h *Halcyon) setupSubs() error {
	if h == nil {
		return ErrBadInst
	}

	prefix := "halcyon.rpc." + h.serviceName
	h.log.Info("subscribing for service rpc on ", prefix)
	_, err := h.conn.QueueSubscribe(prefix+".>", prefix, func(msg *nats.Msg) {
		res, err := h.router.Exec(context.Background(), msg.Subject, msg.Data)
		if err != nil {
			h.log.WithError(err).Error("request execution fialed")
			res = []byte(err.Error())
		}

		if res == nil || msg.Reply == "" {
			return
		}
		h.conn.Publish(msg.Reply, res.([]byte))
	})

	if err != nil {
		h.log.WithError(err).Error("Subscripting for request failed")
		return err
	}

	h.log.Info("subscribing interested notifications")
	for _, serviceName := range h.conf.NotificationTopics {
		nprefix := "halcyon.notify." + serviceName
		h.log.Info("subscribing for notification from: ", nprefix)
		_, err := h.conn.Subscribe(nprefix+".>", func(msg *nats.Msg) {
			sub := strings.TrimPrefix(msg.Subject, "halcyon.")
			h.log.Info("incomming notification from: ", sub)
			h.service.Notify(context.Background(), sub, msg.Data)
		})

		if err != nil {
			h.log.WithError(err).Error("Subscripting for failed")
			return err
		}
	}

	return nil
}

func (h *Halcyon) Run() error {

	if h == nil {
		return ErrBadInst
	}

	h.log.Info("Connecting to configured NATS cluster")
	h.connect()

	h.cl.cl = h.conn

	h.log.Infof("Initializing Router for halcyon.rpc.%s", h.serviceName)
	h.router = sr.NewRouter(
		sr.RootPrefix(fmt.Sprintf("halcyon.rpc.%s.", h.serviceName)),
		sr.RouteCallback(func(path string, route *sr.Route) {
			h.log.Infof("request '%s' matched to %s", path, route.Name())
		}),
	)

	h.log.Info("initialzing service handler")
	if err := h.service.Init(h.router, h.cl); err != nil {
		return err
	}

	if err := h.setupSubs(); err != nil {
		return err
	}

	h.log.Info("init complete...waiting for shutdown command")
	for shutdown := range h.done {
		if shutdown == true {
			break
		}
	}

	return nil
}

func (h *Halcyon) Shutdown() {
	if h == nil {
		return
	}

	h.done <- true
	close(h.done)
}
