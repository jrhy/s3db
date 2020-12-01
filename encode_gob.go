package s3db

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

func marshalGob(thing interface{}) ([]byte, error) {
	var network bytes.Buffer
	enc := gob.NewEncoder(&network)
	err := enc.Encode(thing)
	if err != nil {
		return nil, fmt.Errorf("encode gob: %w", err)
	}
	return network.Bytes(), nil
}

func unmarshalGob(input []byte, thing interface{}) error {
	dec := gob.NewDecoder(bytes.NewBuffer(input))
	err := dec.Decode(thing)
	if err != nil {
		return fmt.Errorf("decode gob: %w", err)
	}
	return nil
}
