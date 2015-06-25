package seal

import (
	"fmt"
	"github.com/mailgun/vulcand/engine"
	"github.com/mailgun/vulcand/secret"
)

type SealedHostEntry struct {
	Name     string
	Settings SealedHostSettings
}

type SealedHostSettings struct {
	Default       bool
	SealedKeyPair []byte
	OCSP          engine.OCSPSettings
}

func SealHost(box *secret.Box, host *engine.Host) (*SealedHostEntry, error) {
	unsealedKeyPair := host.Settings.KeyPair

	var (
		sealedKeyPair []byte
		err           error
	)

	if unsealedKeyPair != nil {
		if box == nil {
			return nil, fmt.Errorf("this backend does not support encryption")
		}
		sealedKeyPair, err = secret.SealKeyPairToJSON(box, unsealedKeyPair)
		if err != nil {
			return nil, err
		}
	} else {
		sealedKeyPair = nil
	}

	return &SealedHostEntry{
		Name: host.Name,
		Settings: SealedHostSettings{
			Default:       host.Settings.Default,
			SealedKeyPair: sealedKeyPair,
			OCSP:          host.Settings.OCSP,
		},
	}, nil
}

func UnsealHost(box *secret.Box, host *SealedHostEntry) (*engine.Host, error) {
	sealedKeyPair := host.Settings.SealedKeyPair

	var (
		unsealedKeyPair *engine.KeyPair
		err             error
	)

	if sealedKeyPair != nil {
		if box == nil {
			return nil, fmt.Errorf("need secretbox to open sealed data")
		}
		unsealedKeyPair, err = secret.UnsealKeyPairFromJSON(box, sealedKeyPair)
		if err != nil {
			return nil, err
		}
	} else {
		unsealedKeyPair = nil
	}

	return engine.NewHost(host.Name,
		engine.HostSettings{
			Default: host.Settings.Default,
			KeyPair: unsealedKeyPair,
			OCSP:    host.Settings.OCSP,
		})
}
