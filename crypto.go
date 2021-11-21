package s3db

import (
	"encoding/base64"
	"errors"
	"fmt"

	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/blake2b"
	"golang.org/x/crypto/poly1305"
	"golang.org/x/crypto/salsa20"
	"golang.org/x/crypto/salsa20/salsa"
)

const (
	macLen                     = 16
	encryptNonceLen            = 24
	deriveKeySaltLen           = 16
	keyLen                     = 32
	crypto_secretbox_zerobytes = 16 + macLen
)

var ErrMACVerificationFailure = errors.New("MAC verification failure")

func encrypt(key *[32]byte, message []byte) ([]byte, error) {
	combined := make([]byte, 0, len(message)+len(key))
	combined = append(combined, message...)
	combined = append(combined, key[:]...)
	n, err := nonce(combined, encryptNonceLen)
	if err != nil {
		return nil, err
	}
	c, err := crypto_secretbox_easy(message, n, key)
	if err != nil {
		return nil, err
	}
	return append(n, c...), nil
}

func decrypt(key *[32]byte, c []byte) ([]byte, error) {
	if len(c) < encryptNonceLen {
		return nil, fmt.Errorf("message too short, no nonce")
	}
	m, err := crypto_secretbox_open_easy(c[24:], c[0:24], key)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func nonce(message []byte, nonce_len int) ([]byte, error) {
	hasher, err := blake2b.New(nonce_len, nil)
	if err != nil {
		return nil, err
	}
	_, err = hasher.Write(message)
	if err != nil {
		return nil, err
	}
	return hasher.Sum(nil), nil
}

func crypto_secretbox_open_detached(
	m []byte,
	c []byte,
	mac []byte,
	n []byte,
	k *[32]byte) error {
	var subkey [32]byte
	var nonce16 [16]byte
	copy(nonce16[:], n[0:16])
	salsa.HSalsa20(&subkey, &nonce16, k, &salsa.Sigma)
	block0 := make([]byte, 64)
	salsa20.XORKeyStream(block0[0:32], block0[0:32], n[16:24], &subkey)
	var lmac [16]byte
	copy(lmac[:], mac[:])
	var block0key [32]byte
	copy(block0key[:], block0[:])
	if !poly1305.Verify(&lmac, c, &block0key) {
		return ErrMACVerificationFailure
	}
	mlen0 := len(m)
	if mlen0 > 64-crypto_secretbox_zerobytes {
		mlen0 = 64 - crypto_secretbox_zerobytes
	}
	for i := 0; i < mlen0; i++ {
		block0[i+crypto_secretbox_zerobytes] = c[i]
	}
	blen := mlen0 + crypto_secretbox_zerobytes
	salsa20.XORKeyStream(block0[:blen], block0[:blen], n[16:24], &subkey)
	for i := 0; i < mlen0; i++ {
		m[i] = block0[crypto_secretbox_zerobytes+i]
	}
	if len(c) > mlen0 {
		salsa20.XORKeyStream(m[mlen0:], c[mlen0:], n[16:24], &subkey)
	}
	return nil
}

func crypto_secretbox_open_easy(c []byte, n []byte, k *[32]byte) ([]byte, error) {
	if len(c) < macLen {
		return nil, fmt.Errorf("too short for MAC")
	}
	m := make([]byte, len(c)-16)
	err := crypto_secretbox_open_detached(m, c[16:], c[0:16], n, k)
	if err != nil {
		return nil, err
	}
	return m, nil
}

// TODO: switch to golang.org/x/crypto/nacl/secretbox since it exists
func crypto_secretbox_easy(m []byte, n []byte, k *[32]byte) ([]byte, error) {
	c := make([]byte, len(m)+macLen)
	err := crypto_secretbox_detached(c[16:], c[0:16], m, n, k)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func crypto_secretbox_detached(c []byte, mac []byte, m []byte, n []byte,
	k *[32]byte) error {
	var subkey [32]byte
	var nonce16 [16]byte
	copy(nonce16[:], n[0:16])
	salsa.HSalsa20(&subkey, &nonce16, k, &salsa.Sigma)
	if len(c) != len(m) {
		return fmt.Errorf("ciphertext buffer must be same size as message")
	}
	mlen0 := len(m)
	if mlen0 > 64-crypto_secretbox_zerobytes {
		mlen0 = 64 - crypto_secretbox_zerobytes
	}
	block0 := make([]byte, 64)
	for i := 0; i < mlen0; i++ {
		block0[i+crypto_secretbox_zerobytes] = m[i]
	}
	blen := mlen0 + crypto_secretbox_zerobytes
	salsa20.XORKeyStream(block0[:blen], block0[:blen], n[16:24], &subkey)
	for i := 0; i < mlen0; i++ {
		c[i] = block0[crypto_secretbox_zerobytes+i]
	}
	if len(m) > mlen0 {
		salsa20.XORKeyStream(c[mlen0:], m[mlen0:], n[16:24], &subkey)
	}

	var lmac [16]byte
	var block0key [32]byte
	copy(block0key[:], block0[:])
	poly1305.Sum(&lmac, c, &block0key)
	copy(mac[0:16], lmac[:])
	return nil
}

func V1NodeEncryptor(passphrase []byte) Encryptor {
	var key [32]byte
	copy(key[:], deriveKey(passphrase, nil))
	return &jencryptor{key}
}

type jencryptor struct {
	key [32]byte
}

func (j *jencryptor) Encrypt(path string, value []byte) ([]byte, error) {
	return encrypt(&j.key, value)
}
func (j *jencryptor) Decrypt(path string, value []byte) ([]byte, error) {
	return decrypt(&j.key, value)
}

func deriveKey(master, context []byte) []byte {
	combined := make([]byte, 0, len(context)+len(master))
	combined = append(combined, context...)
	combined = append(combined, master...)
	salt, _ := nonce(combined, deriveKeySaltLen)
	// base64-encode passphrase for compatibility with libsodium-based impls in which pwhash requires NUL-terminated source
	return argon2.IDKey([]byte(base64.StdEncoding.EncodeToString(combined)),
		salt, 1, 8, 1, keyLen)
}
