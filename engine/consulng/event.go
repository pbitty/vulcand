package consulng

import (
	"fmt"
	"github.com/mailgun/vulcand/Godeps/_workspace/src/github.com/hashicorp/consul/api"
	"github.com/mailgun/vulcand/engine"
	"regexp"
)

var (
	hostRegexp     = regexp.MustCompile(".*/hosts/[^/]+$")
	listenerRegexp = regexp.MustCompile(".*/listeners/[^/]+$")
	backendRegexp  = regexp.MustCompile(".*/backends/[^/]+/backend$")
	frontendRegexp = regexp.MustCompile(".*/frontends/[^/]+/frontend")
)

func (n *ng) createEvent(kvPair *api.KVPair, changeType ChangeType) (interface{}, error) {
	parsers := []ChangeParser{
		n.parseHostChange,
		n.parseListenerChange,
		n.parseBackendChange,
		n.parseFrontendChange,
	}

	for _, parser := range parsers {
		event, err := parser(kvPair, changeType)
		if event != nil || err != nil {
			return event, err
		}
	}
	return nil, fmt.Errorf("Unrecognized key: %s [kvPair: %s]", kvPair.Key, kvPair)
}

type ChangeParser func(kvPair *api.KVPair, changeType ChangeType) (interface{}, error)

func (n *ng) parseHostChange(kvPair *api.KVPair, changeType ChangeType) (interface{}, error) {
	if hostRegexp.MatchString(kvPair.Key) {
		host, err := n.createHost(kvPair)
		if err != nil {
			return nil, err
		}

		if changeType == Upsert {
			return &engine.HostUpserted{
				Host: *host,
			}, nil
		} else {
			return &engine.HostDeleted{
				HostKey: engine.HostKey{host.Name},
			}, nil
		}
	}
	return nil, nil
}

func (n *ng) parseListenerChange(kvPair *api.KVPair, changeType ChangeType) (interface{}, error) {
	if listenerRegexp.MatchString(kvPair.Key) {
		listener, err := engine.ListenerFromJSON(kvPair.Value)
		if err != nil {
			return nil, err
		}

		if changeType == Upsert {
			return &engine.ListenerUpserted{
				Listener: *listener,
			}, nil
		} else {
			return &engine.ListenerDeleted{
				ListenerKey: engine.ListenerKey{Id: listener.Id},
			}, nil
		}
	}
	return nil, nil
}

func (n *ng) parseBackendChange(kvPair *api.KVPair, changeType ChangeType) (interface{}, error) {
	if backendRegexp.MatchString(kvPair.Key) {
		backend, err := engine.BackendFromJSON(kvPair.Value)
		if err != nil {
			return nil, err
		}

		if changeType == Upsert {
			return &engine.BackendUpserted{
				Backend: *backend,
			}, nil
		} else {
			return &engine.BackendDeleted{
				BackendKey: engine.BackendKey{Id: backend.Id},
			}, nil
		}
	}
	return nil, nil
}

func (n *ng) parseFrontendChange(kvPair *api.KVPair, changeType ChangeType) (interface{}, error) {
	if frontendRegexp.MatchString(kvPair.Key) {
		frontend, err := engine.FrontendFromJSON(kvPair.Value)
		if err != nil {
			return nil, err
		}

		if changeType == Upsert {
			return &engine.FrontendUpserted{
				Frontend: *frontend,
			}, nil
		} else {
			return &engine.FrontendDeleted{
				FrontendKey: engine.FrontendKey{Id: frontend.Id},
			}, nil
		}
	}
	return nil, nil
}
