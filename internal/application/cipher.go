package application

import (
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"io"

	"golang.org/x/crypto/chacha20poly1305"
)

type messageCipher struct {
	aead cipher.AEAD
	rand io.Reader
}

// NewMessageCipher constructs the chat text cipher backed by ChaCha20-Poly1305.
func NewMessageCipher(secret string) (MessageCipher, error) {
	sum := sha256.Sum256([]byte(secret))
	aead, err := chacha20poly1305.New(sum[:])
	if err != nil {
		return nil, err
	}
	return &messageCipher{aead: aead, rand: rand.Reader}, nil
}

func (c *messageCipher) Encrypt(plain string) (string, error) {
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := io.ReadFull(c.rand, nonce); err != nil {
		return "", err
	}
	out := c.aead.Seal(nonce, nonce, []byte(plain), nil)
	return base64.RawURLEncoding.EncodeToString(out), nil
}

func (c *messageCipher) Decrypt(ciphertext string) (string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", err
	}
	nonce := raw[:c.aead.NonceSize()]
	payload := raw[c.aead.NonceSize():]
	plain, err := c.aead.Open(nil, nonce, payload, nil)
	if err != nil {
		return "", err
	}
	return string(plain), nil
}
