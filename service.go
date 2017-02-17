package halcyon

import (
	"errors"
	"github.com/Sirupsen/logrus"
	sr "github.com/codmajik/servicerouter"
	nats "github.com/nats-io/go-nats"
	"github.com/nats-io/nuid"
	"golang.org/x/net/context"
	"regexp"
	"strings"
	"time"
)

var serviceNameSanitize, _ = regexp.Compile("[^a-zA-Z0-9]")
var ErrBadInst = errors.New("Halcyon: not instantianted")

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
		NatsUri:            make([]string, 0),
		NotificationTopics: []string{"system"},
	}

	for _, opt := range opts {
		opt(cfg)
	}

	return &Halcyon{
		conf:        cfg,
		serviceName: strings.ToLower(serviceNameSanitize.ReplaceAllString(cfg.ServiceName, "")),
		service:     srv,
		done:        make(chan bool),
		cl:          &Client{},
		log: logrus.WithFields(logrus.Fields{
			"service": cfg.ServiceName,
			"ctx":     "halcyon",
		}),
	}
}

func (h *Halcyon) connect() {
	clName := "halcyon.service." + h.serviceName + "-" + nuid.Next()
	timeout := int64(0)
	for {
		time.Sleep(time.Duration(timeout) * time.Second)
		conn, err := nats.Connect(
			strings.Join(h.conf.NatsUri, ","),
			nats.DisconnectHandler(func(c *nats.Conn) {
				h.log.Error("disconnected from NATS cluster...trying again!!!")
			}),
			nats.ReconnectHandler(func(c *nats.Conn) {
				h.log.Error("connected to NATS cluster")
			}),
			nats.Name(clName),
			nats.MaxReconnects(-1),
		)
		if err != nil {
			h.log.Info("connection attempt failed...trying", err.Error())

			if timeout*2 > 20 {
				timeout = 20
			} else {
				timeout = (timeout * 2) + 1
			}

			continue
		}

		h.conn = conn
		h.log.Info("connected to NATS server with ID: ", conn.ConnectedServerId())
		break
	}
}

func (h *Halcyon) setupSubs() error {
	prefix := "halcyon." + h.serviceName + ".service"
	h.log.Info("subscribing for service requests on ", prefix)
	_, err := h.conn.QueueSubscribe(prefix+".>", prefix, func(msg *nats.Msg) {
		res, err := h.router.Exec(context.Background(), msg.Subject, msg.Data)
		if err != nil {
			h.log.Error("executing request failed with", err)
			h.conn.Publish(msg.Reply, []byte(err.Error()))
			return
		}
		if res == nil || msg.Reply == "" {
			return
		}
		h.conn.Publish(msg.Reply, res.([]byte))
	})

	if err != nil {
		h.log.Error("Subscripting for request failed")
		return err
	}

	h.log.Info("subscribing interested notifications")
	for _, serviceName := range h.conf.NotificationTopics {
		nprefix := "halcyon." + serviceName + ".notify"
		h.log.Info("subscribing for notification from", nprefix)
		_, err := h.conn.Subscribe(nprefix+".>", func(msg *nats.Msg) {
			sub := strings.TrimPrefix(msg.Subject, "halcyon.")
			h.log.Info("incomming notification from", sub)
			h.service.Notify(context.Background(), sub, msg.Data)
		})

		if err != nil {
			h.log.Error("Subscripting for failed", err)
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

	h.log.Infof("Initializing Router for halcyon.%s.service", h.serviceName)
	h.router = sr.NewRouter(
		sr.RootPrefix("halcyon."+h.serviceName+".service."),
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
