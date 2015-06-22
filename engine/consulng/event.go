package consulng

import (
	"fmt"
	"github.com/mailgun/vulcand/Godeps/_workspace/src/github.com/hashicorp/consul/api"
	"github.com/mailgun/vulcand/engine"
	"regexp"
)

var (
	hostRegexp = regexp.MustCompile(".*/hosts/[^/]+/host")
)

func (n *ng) createEvent(kvPair *api.KVPair, changeType ChangeType) (interface{}, error) {
	parsers := []ChangeParser{
		n.parseHostChange,
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

		var event interface{}
		if changeType == Upsert {
			event = &engine.HostUpserted{
				Host: *host,
			}
		} else {
			event = &engine.HostDeleted{
				HostKey: engine.HostKey{host.Name},
			}
		}
		return event, nil
	}
	return nil, nil
}
