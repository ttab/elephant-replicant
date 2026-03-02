package internal_test

import (
	"encoding/hex"
	"testing"

	"github.com/ttab/elephant-replicant/internal"
)

func TestEncryptDecryptRoundTrip(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}

	plaintext := "my-super-secret-client-secret"

	encrypted, err := internal.EncryptSecret(key, plaintext)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	if encrypted == plaintext {
		t.Fatal("encrypted value should differ from plaintext")
	}

	decrypted, err := internal.DecryptSecret(key, encrypted)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}

	if decrypted != plaintext {
		t.Fatalf("got %q, want %q", decrypted, plaintext)
	}
}

func TestEncryptProducesDifferentCiphertexts(t *testing.T) {
	key := make([]byte, 32)

	a, err := internal.EncryptSecret(key, "secret")
	if err != nil {
		t.Fatalf("encrypt a: %v", err)
	}

	b, err := internal.EncryptSecret(key, "secret")
	if err != nil {
		t.Fatalf("encrypt b: %v", err)
	}

	if a == b {
		t.Fatal("two encryptions of the same plaintext should produce different ciphertexts")
	}
}

func TestDecryptWrongKeyFails(t *testing.T) {
	key1 := make([]byte, 32)
	key2 := make([]byte, 32)
	key2[0] = 0xff

	encrypted, err := internal.EncryptSecret(key1, "secret")
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	_, err = internal.DecryptSecret(key2, encrypted)
	if err == nil {
		t.Fatal("expected decryption to fail with wrong key")
	}
}

func TestParseEncryptionKey(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}

	hexKey := hex.EncodeToString(key)

	parsed, err := internal.ParseEncryptionKey(hexKey)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if len(parsed) != 32 {
		t.Fatalf("got %d bytes, want 32", len(parsed))
	}
}

func TestParseEncryptionKeyRejectsShortInput(t *testing.T) {
	_, err := internal.ParseEncryptionKey("abcd")
	if err == nil {
		t.Fatal("expected error for short key")
	}
}

func TestParseEncryptionKeyRejectsInvalidHex(t *testing.T) {
	_, err := internal.ParseEncryptionKey(
		"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
	)
	if err == nil {
		t.Fatal("expected error for invalid hex")
	}
}
