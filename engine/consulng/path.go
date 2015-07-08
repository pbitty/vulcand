package consulng

import (
	"github.com/mailgun/vulcand/engine"
)

func (n *ng) hostKeyPath(k engine.HostKey) string {
	return n.path("hosts", k.Name)
}

func (n *ng) hostPath(h engine.Host) string {
	return n.path("hosts", h.Name)
}

func (n *ng) hostsPath() string {
	return n.path("hosts")
}

func (n *ng) listenerKeyPath(l engine.ListenerKey) string {
	return n.path("listeners", l.Id)
}

func (n *ng) listenerPath(l engine.Listener) string {
	return n.path("listeners", l.Id)
}

func (n *ng) listenersPath() string {
	return n.path("listeners")
}

func (n *ng) backendPath(b engine.Backend) string {
	return n.path("backends", b.Id, "backend")
}

func (n *ng) backendKeyPath(b engine.BackendKey) string {
	return n.path("backends", b.Id, "backend")
}

func (n *ng) backendsPath() string {
	return n.path("backends")
}

func (n *ng) frontendPath(f engine.Frontend) string {
	return n.path("frontends", f.Id, "frontend")
}

func (n *ng) frontendKeyPath(f engine.FrontendKey) string {
	return n.path("frontends", f.Id, "frontend")
}

func (n *ng) frontendsPath() string {
	return n.path("frontends")
}
