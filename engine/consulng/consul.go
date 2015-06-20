package consulng

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mailgun/vulcand/Godeps/_workspace/src/github.com/hashicorp/consul/api"
	"github.com/mailgun/vulcand/engine"
	"github.com/mailgun/vulcand/plugin"
	"regexp"
	"strings"
	"time"
)

var (
	hostKeyRegexp = regexp.MustCompile(".*/hosts/[^/]+/host")
)

type ChangeType int

const (
	Upsert ChangeType = iota
	Delete
)

type ng struct {
	client     *api.Client
	prefix     string
	localState map[string]*api.KVPair
}

func New(hostAddress string, prefix string) (engine.Engine, error) {
	client, err := api.NewClient(&api.Config{
		Address: hostAddress,
	})
	if err != nil {
		return nil, err
	}

	return &ng{
		client:     client,
		prefix:     prefix,
		localState: map[string]*api.KVPair{},
	}, nil
}

func (n *ng) UpsertHost(h engine.Host) error {
	fmt.Println("Upserting")
	if h.Name == "" {
		return &engine.InvalidFormatError{Message: "hostname cannot be empty"}
	}

	key := n.path("hosts", h.Name, "host")
	value := h

	// TODO Implement sealing TLS keys

	return n.putJSON(key, value)
}

func (n *ng) Subscribe(events chan interface{}, cancel chan bool) error {
	fmt.Println("Subscribing")
	// TODO implement cancel functionality
	waitIndex := uint64(1)
	for {
		// wait for changes
		pairs, queryMeta, err := n.client.KV().List(n.prefix, &api.QueryOptions{
			WaitIndex: waitIndex,
		})
		fmt.Println("Got changes")
		if err != nil {
			fmt.Println("Change errors")
			return err
		}
		fmt.Println("Changes: %s", pairs)
		waitIndex = queryMeta.LastIndex

		remoteState := mapByKey(pairs)
		upserts := n.syncUpserts(remoteState)
		fmt.Println("Upserts: %v (%s)", upserts, len(upserts))
		for _, upsertPair := range upserts {
			fmt.Println("upsertPair: %#v", upsertPair)
			// _ = "breakpoint"
			if event, err := toEvent(upsertPair, Upsert); err == nil {
				events <- event
			} else {
				return err
			}
		}

		deletes := n.syncDeletes(remoteState)
		for _, deletePair := range deletes {
			if event, err := toEvent(deletePair, Delete); err == nil {
				events <- event
			} else {
				return err
			}
		}
	}
}

func toEvent(kvPair *api.KVPair, changeType ChangeType) (event interface{}, err error) {
	if hostKeyRegexp.MatchString(kvPair.Key) {
		host := &engine.Host{}
		if err := json.Unmarshal(kvPair.Value, host); err == nil {
			if changeType == Upsert {
				return &engine.HostUpserted{
					Host: *host,
				}, nil
			} else {
				return &engine.HostDeleted{
					HostKey: engine.HostKey{host.Name},
				}, nil
			}

		} else {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("Unrecognized key: %s [kvPair: %s]", kvPair.Key, kvPair)
	}
}

func (n *ng) syncUpserts(remoteState map[string]*api.KVPair) []*api.KVPair {
	upserts := []*api.KVPair{}
	for key, remotePair := range remoteState {
		localPair, exists := n.localState[key]
		if !exists || string(remotePair.Value) != string(localPair.Value) {
			n.localState[key] = remotePair
			upserts = append(upserts, remotePair)
		}
	}
	return upserts
}

func (n *ng) syncDeletes(remoteState map[string]*api.KVPair) []*api.KVPair {
	deletes := []*api.KVPair{}
	for key, localPair := range n.localState {
		if _, exists := remoteState[key]; !exists {
			delete(n.localState, key)
			deletes = append(deletes, localPair)
		}
	}
	return deletes
}

func mapByKey(pairs []*api.KVPair) map[string]*api.KVPair {
	pairsByKey := map[string]*api.KVPair{}
	for _, pair := range pairs {
		pairsByKey[pair.Key] = pair
	}
	return pairsByKey
}

func (n *ng) putJSON(key string, value interface{}) error {
	bytes, err := json.Marshal(value)
	if err != nil {
		return err
	}

	kvPair := &api.KVPair{
		Key:   key,
		Value: bytes,
	}
	_, err = n.client.KV().Put(kvPair, nil)
	if err != nil {
		return err
	}
	return nil
}

func (n *ng) path(keys ...string) string {
	return strings.Join(append([]string{n.prefix}, keys...), "/")
}

//
// Copy-pasted stuff from etcdng
// TODO refactor
//
type hostEntry struct {
	Name     string
	Settings hostSettings
}

type hostSettings struct {
	Default bool
	KeyPair []byte
	OCSP    engine.OCSPSettings
}

//
// Not yet implemented ...
//
func (n *ng) GetHosts() ([]engine.Host, error) {
	return nil, errors.New("Not yet implemented")
}

func (n *ng) GetHost(engine.HostKey) (*engine.Host, error) {
	return nil, errors.New("Not yet implemented")
}

func (n *ng) DeleteHost(engine.HostKey) error {
	return errors.New("Not yet implemented")
}

func (n *ng) GetListeners() ([]engine.Listener, error) {
	return nil, errors.New("Not yet implemented")
}

func (n *ng) GetListener(engine.ListenerKey) (*engine.Listener, error) {
	return nil, errors.New("Not yet implemented")
}

func (n *ng) UpsertListener(engine.Listener) error {
	return errors.New("Not yet implemented")
}

func (n *ng) DeleteListener(engine.ListenerKey) error {
	return errors.New("Not yet implemented")
}

func (n *ng) GetFrontends() ([]engine.Frontend, error) {
	return nil, errors.New("Not yet implemented")
}

func (n *ng) GetFrontend(engine.FrontendKey) (*engine.Frontend, error) {
	return nil, errors.New("Not yet implemented")
}

func (n *ng) UpsertFrontend(engine.Frontend, time.Duration) error {
	return errors.New("Not yet implemented")
}

func (n *ng) DeleteFrontend(engine.FrontendKey) error {
	return errors.New("Not yet implemented")
}

func (n *ng) GetMiddlewares(engine.FrontendKey) ([]engine.Middleware, error) {
	return nil, errors.New("Not yet implemented")
}

func (n *ng) GetMiddleware(engine.MiddlewareKey) (*engine.Middleware, error) {
	return nil, errors.New("Not yet implemented")
}

func (n *ng) UpsertMiddleware(engine.FrontendKey, engine.Middleware, time.Duration) error {
	return errors.New("Not yet implemented")
}

func (n *ng) DeleteMiddleware(engine.MiddlewareKey) error {
	return errors.New("Not yet implemented")
}

func (n *ng) GetBackends() ([]engine.Backend, error) {
	return nil, errors.New("Not yet implemented")
}

func (n *ng) GetBackend(engine.BackendKey) (*engine.Backend, error) {
	return nil, errors.New("Not yet implemented")
}

func (n *ng) UpsertBackend(engine.Backend) error {
	return errors.New("Not yet implemented")
}

func (n *ng) DeleteBackend(engine.BackendKey) error {
	return errors.New("Not yet implemented")
}

func (n *ng) GetServers(engine.BackendKey) ([]engine.Server, error) {
	return nil, errors.New("Not yet implemented")
}

func (n *ng) GetServer(engine.ServerKey) (*engine.Server, error) {
	return nil, errors.New("Not yet implemented")
}

func (n *ng) UpsertServer(engine.BackendKey, engine.Server, time.Duration) error {
	return errors.New("Not yet implemented")
}

func (n *ng) DeleteServer(engine.ServerKey) error {
	return errors.New("Not yet implemented")
}

func (n *ng) GetRegistry() *plugin.Registry {
	panic("Not yet implemented")
}

func (n *ng) Close() {
}
