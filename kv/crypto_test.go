package kv

import (
	"encoding/base64"

	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/nacl/secretbox"
)

func TestNonce(t *testing.T) {
	testKey, err := base64.StdEncoding.DecodeString("UdHBz8klP8ze+cl+qP2zcFBOW952mo8DUc/tn59h6Rw=")
	require.NoError(t, err)
	input1 := []byte("asdf")
	input := append(input1, testKey...)
	hash, err := nonce(input, 24)
	require.NoError(t, err)
	require.Equal(t, "DuO9oCKfeLUrcIImvVH88Y67un3CFnRw", base64.StdEncoding.EncodeToString(hash))
}

func TestDeriveKey(t *testing.T) {
	testKey, err := base64.StdEncoding.DecodeString("UdHBz8klP8ze+cl+qP2zcFBOW952mo8DUc/tn59h6Rw=")
	require.NoError(t, err)
	derived := deriveKey(testKey, []byte("foo"))
	require.Equal(t, "7a0p0qOL3IqOBMPwjUlGokjz8FNDQDedZRXom5ii/Ls=", base64.StdEncoding.EncodeToString(derived))
}

func TestEncrypt(t *testing.T) {
	testKey_slice, err := base64.StdEncoding.DecodeString("UdHBz8klP8ze+cl+qP2zcFBOW952mo8DUc/tn59h6Rw=")
	require.NoError(t, err)
	var testKey [32]byte
	copy(testKey[:], testKey_slice[:32])
	encrypted, err := encrypt(&testKey, []byte("asdf"))
	require.NoError(t, err)
	require.Equal(t, "DuO9oCKfeLUrcIImvVH88Y67un3CFnRwhZOvsmKMKFjTuKYsiLv0bwSBbjo=",
		base64.StdEncoding.EncodeToString(encrypted))
}

func TestSecretBoxCompat(t *testing.T) {
	testKey_slice, err := base64.StdEncoding.DecodeString("UdHBz8klP8ze+cl+qP2zcFBOW952mo8DUc/tn59h6Rw=")
	require.NoError(t, err)
	var testKey [32]byte
	copy(testKey[:], testKey_slice[:32])
	nonceBytes, err := nonce([]byte("asdf"), 24)
	require.NoError(t, err)
	var nonce [24]byte
	copy(nonce[:], nonceBytes[:24])

	encryptedLocal, err := crypto_secretbox_easy([]byte("asdf"), nonceBytes, &testKey)
	assert.Equal(t, "1VyPASCeHN/X2MVLxdYEUmMVjTQ=",
		base64.StdEncoding.EncodeToString(encryptedLocal))

	var encrypted []byte
	encrypted = secretbox.Seal(encrypted, []byte("asdf"), &nonce, &testKey)
	assert.Equal(t, "1VyPASCeHN/X2MVLxdYEUmMVjTQ=",
		base64.StdEncoding.EncodeToString(encrypted))
}

func TestDecrypt(t *testing.T) {
	testKey_slice, err := base64.StdEncoding.DecodeString("UdHBz8klP8ze+cl+qP2zcFBOW952mo8DUc/tn59h6Rw=")
	require.NoError(t, err)
	var testKey [32]byte
	copy(testKey[:], testKey_slice[:32])

	decoded := make([]byte, 100)
	decodedLen, err := base64.StdEncoding.Decode(
		decoded,
		[]byte("DuO9oCKfeLUrcIImvVH88Y67un3CFnRwhZOvsmKMKFjTuKYsiLv0bwSBbjo="))
	require.NoError(t, err)

	decrypted, err := decrypt(&testKey, decoded[0:decodedLen])
	require.NoError(t, err)
	require.Equal(t, "asdf", string(decrypted))
}
