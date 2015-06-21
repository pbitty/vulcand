package consulng

import (
	"github.com/mailgun/vulcand/Godeps/_workspace/src/github.com/hashicorp/consul/api"
	"github.com/mailgun/vulcand/Godeps/_workspace/src/github.com/mailgun/log"
	"github.com/mailgun/vulcand/engine/test"
	"github.com/mailgun/vulcand/secret"
	"os"
	"testing"

	. "github.com/mailgun/vulcand/Godeps/_workspace/src/gopkg.in/check.v1"
)

func TestConsul(t *testing.T) { TestingT(t) }

type ConsulSuite struct {
	suite        test.EngineSuite
	consulClient *api.Client
	consulHost   string
	consulPrefix string
	stopC        chan bool
	key          string
}

var _ = Suite(&ConsulSuite{
	consulPrefix: "consultest",
})

func (s *ConsulSuite) SetUpSuite(c *C) {
	log.Init([]*log.LogConfig{&log.LogConfig{Name: "console"}})

	consulHost := os.Getenv("VULCAND_TEST_CONSUL_HOST")
	if consulHost == "" {
		c.Skip("This test requires consul, provide host in VULCAND_TEST_CONSUL_HOST environment variable")
	}

	if consulClient, err := api.NewClient(&api.Config{Address: consulHost}); err == nil {
		s.consulClient = consulClient
	} else {
		c.Fatalf("Error creating consul client: %s", err)
		return
	}
	s.consulHost = consulHost

	key, err := secret.NewKeyString()
	if err != nil {
		panic(err)
	}
	s.key = key
}

func (s *ConsulSuite) SetUpTest(c *C) {
	key, err := secret.KeyFromString(s.key)
	c.Assert(err, IsNil)

	box, err := secret.NewBox(key)
	c.Assert(err, IsNil)

	engine, err := New(s.consulHost, s.consulPrefix, box)
	c.Assert(err, IsNil)

	if _, err := s.consulClient.KV().DeleteTree(s.consulPrefix, nil); err != nil {
		c.Fatalf("Error deleting KV subtree at prefix %s: %s", s.consulPrefix, err)
	}

	s.suite.ChangesC = make(chan interface{})
	s.stopC = make(chan bool)
	go func() {
		if err := engine.Subscribe(s.suite.ChangesC, s.stopC); err != nil {
			c.Fatalf("Error from engine: %s", err)
		}
	}()

	s.suite.Engine = engine
}

func (s *ConsulSuite) TearDownTest(c *C) {
}

func (s *ConsulSuite) TestEmptyParams(c *C) {
	s.suite.EmptyParams(c)
}

func (s *ConsulSuite) TestHostCRUD(c *C) {
	s.suite.HostCRUD(c)
}

func (s *ConsulSuite) TestHostWithKeyPair(c *C) {
	s.suite.HostWithKeyPair(c)
}

func (s *ConsulSuite) TestHostUpsertKeyPair(c *C) {
	s.suite.HostUpsertKeyPair(c)
}

func (s *ConsulSuite) TestHostWithOCSP(c *C) {
	s.suite.HostWithOCSP(c)
}

func (s *ConsulSuite) TestListenerCRUD(c *C) {
	s.suite.ListenerCRUD(c)
}

func (s *ConsulSuite) TestListenerSettingsCRUD(c *C) {
	s.suite.ListenerSettingsCRUD(c)
}

func (s *ConsulSuite) TestBackendCRUD(c *C) {
	s.suite.BackendCRUD(c)
}

func (s *ConsulSuite) TestBackendDeleteUsed(c *C) {
	s.suite.BackendDeleteUsed(c)
}

func (s *ConsulSuite) TestBackendDeleteUnused(c *C) {
	s.suite.BackendDeleteUnused(c)
}

func (s *ConsulSuite) TestServerCRUD(c *C) {
	s.suite.ServerCRUD(c)
}

func (s *ConsulSuite) TestServerExpire(c *C) {
	s.suite.ServerExpire(c)
}

func (s *ConsulSuite) TestFrontendCRUD(c *C) {
	s.suite.FrontendCRUD(c)
}

func (s *ConsulSuite) TestFrontendExpire(c *C) {
	s.suite.FrontendExpire(c)
}

func (s *ConsulSuite) TestFrontendBadBackend(c *C) {
	s.suite.FrontendBadBackend(c)
}

func (s *ConsulSuite) TestMiddlewareCRUD(c *C) {
	s.suite.MiddlewareCRUD(c)
}

func (s *ConsulSuite) TestMiddlewareExpire(c *C) {
	s.suite.MiddlewareExpire(c)
}

func (s *ConsulSuite) TestMiddlewareBadFrontend(c *C) {
	s.suite.MiddlewareBadFrontend(c)
}

func (s *ConsulSuite) TestMiddlewareBadType(c *C) {
	s.suite.MiddlewareBadType(c)
}
