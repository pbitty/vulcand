package consulng

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mailgun/vulcand/Godeps/_workspace/src/github.com/hashicorp/consul/api"
	"github.com/mailgun/vulcand/engine"
	"github.com/mailgun/vulcand/plugin"
	"github.com/mailgun/vulcand/secret"
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
	box        *secret.Box
	localState map[string]*api.KVPair
}

func New(hostAddress string, prefix string, box *secret.Box) (engine.Engine, error) {
	if client, err := api.NewClient(&api.Config{Address: hostAddress}); err == nil {
		return &ng{
			client:     client,
			prefix:     prefix,
			box:        box,
			localState: map[string]*api.KVPair{},
		}, nil
	} else {
		return nil, err
	}
}

func (n *ng) UpsertHost(h engine.Host) error {
	if h.Name == "" {
		return &engine.InvalidFormatError{Message: "hostname cannot be empty"}
	}
	sealedHost, err := n.sealHost(&h)
	if err != nil {
		return err
	}
	return n.putJSON(n.path("hosts", h.Name, "host"), sealedHost)
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
			event, err := n.toEvent(upsertPair, Upsert)
			if err != nil {
				return err
			}
			events <- event
		}

		deletes := n.syncDeletes(remoteState)
		for _, deletePair := range deletes {
			event, err := n.toEvent(deletePair, Delete)
			if err != nil {
				return err
			}
			events <- event
		}
	}
}

func (n *ng) toEvent(kvPair *api.KVPair, changeType ChangeType) (event interface{}, err error) {
	if hostKeyRegexp.MatchString(kvPair.Key) {
		if host, err := n.makeHost(kvPair); err == nil {
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

func (n *ng) makeHost(kvPair *api.KVPair) (*engine.Host, error) {
	var sealedHost *sealedHostEntry
	if err := json.Unmarshal(kvPair.Value, &sealedHost); err != nil {
		return nil, err
	}
	return n.unsealHost(sealedHost)
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
// Repeated logic from etcdng
// TODO refactor
//
type sealedHostEntry struct {
	Name     string
	Settings sealedHostSettings
}

type sealedHostSettings struct {
	Default       bool
	SealedKeyPair []byte
	OCSP          engine.OCSPSettings
}

func (n *ng) sealHost(host *engine.Host) (*sealedHostEntry, error) {
	keyPair, err := n.sealKeyPair(host.Settings.KeyPair)
	if err != nil {
		return nil, err
	}

	return &sealedHostEntry{
		Name: host.Name,
		Settings: sealedHostSettings{
			Default:       host.Settings.Default,
			SealedKeyPair: keyPair,
			OCSP:          host.Settings.OCSP,
		},
	}, nil
}

func (n *ng) unsealHost(host *sealedHostEntry) (*engine.Host, error) {
	keyPair, err := n.unsealKeyPair(host.Settings.SealedKeyPair)
	if err != nil {
		return nil, err
	}

	return &engine.Host{
		Name: host.Name,
		Settings: engine.HostSettings{
			Default: host.Settings.Default,
			KeyPair: keyPair,
			OCSP:    host.Settings.OCSP,
		},
	}, nil
}

func (n *ng) sealKeyPair(unsealedKeyPair *engine.KeyPair) ([]byte, error) {
	if unsealedKeyPair != nil {
		if n.box == nil {
			return nil, fmt.Errorf("this backend does not support encryption")
		}

		sealedKeyPair, err := secret.SealKeyPairToJSON(n.box, unsealedKeyPair)
		if err != nil {
			return nil, err
		}
		return sealedKeyPair, nil
	}
	return nil, nil
}

func (n *ng) unsealKeyPair(sealedKeyPair []byte) (*engine.KeyPair, error) {
	if sealedKeyPair != nil {
		if n.box == nil {
			return nil, fmt.Errorf("need secretbox to open sealed data")
		}
		unsealedKeyPair, err := secret.UnsealKeyPairFromJSON(n.box, sealedKeyPair)
		if err != nil {
			return nil, err
		}
		return unsealedKeyPair, nil
	}
	return nil, nil
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
