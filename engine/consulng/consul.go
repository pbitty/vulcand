package consulng

import (
	"encoding/json"
	"errors"
	"github.com/mailgun/vulcand/Godeps/_workspace/src/github.com/hashicorp/consul/api"
	"github.com/mailgun/vulcand/engine"
	"github.com/mailgun/vulcand/engine/seal"
	"github.com/mailgun/vulcand/plugin"
	"github.com/mailgun/vulcand/secret"
	"strings"
	"time"
)

type ChangeType int

const (
	Upsert ChangeType = iota
	Delete
)

type ng struct {
	client     *api.Client
	prefix     string
	box        *secret.Box
	localState map[string]*api.KVPair
}

func New(hostAddress string, prefix string, box *secret.Box) (engine.Engine, error) {
	client, err := api.NewClient(&api.Config{Address: hostAddress})
	if err != nil {
		return nil, err
	}
	return &ng{
		client:     client,
		prefix:     prefix,
		box:        box,
		localState: map[string]*api.KVPair{},
	}, nil
}

func (n *ng) UpsertHost(h engine.Host) error {
	if h.Name == "" {
		return &engine.InvalidFormatError{Message: "hostname cannot be empty"}
	}
	sealedHost, err := seal.SealHost(n.box, &h)
	if err != nil {
		return err
	}
	return n.putJSON(n.hostPath(h), sealedHost)
}

func (n *ng) GetHosts() ([]engine.Host, error) {
	hosts := []engine.Host{}
	kvPairs, _, err := n.client.KV().List(n.hostsPath(), nil)
	if err != nil {
		return nil, err
	}

	for _, kvPair := range kvPairs {
		host, err := n.createHost(kvPair)
		if err != nil {
			return nil, err
		}
		hosts = append(hosts, *host)
	}
	return hosts, nil
}

func (n *ng) DeleteHost(h engine.HostKey) error {
	_, err := n.client.KV().Delete(n.hostKeyPath(h), nil)
	return err
}

func (n *ng) GetHost(h engine.HostKey) (*engine.Host, error) {
	kvPair, _, err := n.client.KV().Get(n.hostKeyPath(h), nil)
	if err != nil {
		return nil, err
	}
	return n.createHost(kvPair)
}

func (n *ng) UpsertListener(l engine.Listener) error {
	if l.Id == "" {
		return &engine.InvalidFormatError{Message: "listener id can not be empty"}
	}
	return n.putJSON(n.listenerPath(l), l)
}

func (n *ng) GetListeners() ([]engine.Listener, error) {
	listeners := []engine.Listener{}
	kvPairs, _, err := n.client.KV().List(n.listenersPath(), nil)
	if err != nil {
		return nil, err
	}
	for _, kvPair := range kvPairs {
		listener, err := engine.ListenerFromJSON(kvPair.Value)
		if err != nil {
			return nil, err
		}
		listeners = append(listeners, *listener)
	}
	return listeners, nil
}

func (n *ng) GetListener(l engine.ListenerKey) (*engine.Listener, error) {
	kvPair, _, err := n.client.KV().Get(n.listenerKeyPath(l), nil)
	if err != nil {
		return nil, err
	}
	return engine.ListenerFromJSON(kvPair.Value)
}

func (n *ng) DeleteListener(l engine.ListenerKey) error {
	_, err := n.client.KV().Delete(n.listenerKeyPath(l), nil)
	return err
}

func (n *ng) UpsertBackend(b engine.Backend) error {
	if b.Id == "" {
		return &engine.InvalidFormatError{Message: "backend id can not be empty"}
	}
	return n.putJSON(n.backendPath(b), b)
}

func (n *ng) GetBackends() ([]engine.Backend, error) {
	backends := []engine.Backend{}
	kvPairs, _, err := n.client.KV().List(n.backendsPath(), nil)
	if err != nil {
		return nil, err
	}
	for _, kvPair := range kvPairs {
		backend, err := engine.BackendFromJSON(kvPair.Value)
		if err != nil {
			return nil, err
		}
		backends = append(backends, *backend)
	}
	return backends, nil
}

func (n *ng) GetBackend(b engine.BackendKey) (*engine.Backend, error) {
	kvPair, _, err := n.client.KV().Get(n.backendKeyPath(b), nil)
	if err != nil {
		return nil, err
	}
	return engine.BackendFromJSON(kvPair.Value)
}

func (n *ng) DeleteBackend(b engine.BackendKey) error {
	_, err := n.client.KV().Delete(n.backendKeyPath(b), nil)
	return err
}

//
// Not yet implemented ...
//

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
	panic("Not yet implemented")
}

func (n *ng) Subscribe(events chan interface{}, cancel chan bool) error {
	// TODO implement cancel functionality
	waitIndex := uint64(1)
	for {
		// wait for changes
		pairs, queryMeta, err := n.client.KV().List(n.prefix, &api.QueryOptions{WaitIndex: waitIndex})
		if err != nil {
			return err
		}
		waitIndex = queryMeta.LastIndex

		remoteState := mapByKey(pairs)
		upserts := n.syncUpserts(remoteState)
		for _, upsertPair := range upserts {
			event, err := n.createEvent(upsertPair, Upsert)
			if err != nil {
				return err
			}
			events <- event
		}

		deletes := n.syncDeletes(remoteState)
		for _, deletePair := range deletes {
			event, err := n.createEvent(deletePair, Delete)
			if err != nil {
				return err
			}
			events <- event
		}
	}
}

func (n *ng) createHost(kvPair *api.KVPair) (*engine.Host, error) {
	var sealedHost *seal.SealedHostEntry
	if err := json.Unmarshal(kvPair.Value, &sealedHost); err != nil {
		return nil, err
	}
	return seal.UnsealHost(n.box, sealedHost)
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
