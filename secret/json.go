package secret

import (
	"encoding/json"
	"fmt"
	"github.com/mailgun/vulcand/engine"
)

type sealedValue struct {
	Encryption string
	Value      SealedBytes
}

func SealedValueToJSON(b *SealedBytes) ([]byte, error) {
	data := &sealedValue{
		Encryption: encryptionSecretBox,
		Value:      *b,
	}
	return json.Marshal(&data)
}

func SealedValueFromJSON(bytes []byte) (*SealedBytes, error) {
	var v *sealedValue
	if err := json.Unmarshal(bytes, &v); err != nil {
		return nil, err
	}
	if v.Encryption != encryptionSecretBox {
		return nil, fmt.Errorf("unsupported encryption type: '%s'", v.Encryption)
	}
	return &v.Value, nil
}

func SealKeyPairToJSON(box *Box, keyPair *engine.KeyPair) ([]byte, error) {
	bytes, err := json.Marshal(keyPair)
	if err != nil {
		return nil, fmt.Errorf("failed to JSON encode certificate: %s", bytes)
	}

	sealed, err := box.Seal(bytes)
	if err != nil {
		return nil, err
	}

	return SealedValueToJSON(sealed)
}

func UnsealKeyPairFromJSON(box *Box, sealedKeyPair []byte) (*engine.KeyPair, error) {
	var unsealedKeyPair *engine.KeyPair
	sealed, err := SealedValueFromJSON(sealedKeyPair)
	if err != nil {
		return nil, err
	}
	unsealed, err := box.Open(sealed)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(unsealed, &unsealedKeyPair); err != nil {
		return nil, err
	}
	return unsealedKeyPair, nil
}

const encryptionSecretBox = "secretbox.v1"
