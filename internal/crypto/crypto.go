package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"os"

	"golang.org/x/crypto/curve25519"
)

type KeyFile struct {
	SignPublic  string `json:"sign_public"`
	SignPrivate string `json:"sign_private"`
	EncPrivate  string `json:"enc_private"`
	EncPublic   string `json:"enc_public"`
}

func LoadKeys(path string) (ed25519.PublicKey, ed25519.PrivateKey, []byte, []byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	var kf KeyFile
	if err := json.Unmarshal(data, &kf); err != nil {
		return nil, nil, nil, nil, err
	}
	signPub, err := DecodeKey(kf.SignPublic)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	signPriv, err := DecodeKey(kf.SignPrivate)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	encPriv, err := DecodeKey(kf.EncPrivate)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	encPub, err := DecodeKey(kf.EncPublic)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return ed25519.PublicKey(signPub), ed25519.PrivateKey(signPriv), encPriv, encPub, nil
}

func SaveKeys(path string, signPub ed25519.PublicKey, signPriv ed25519.PrivateKey, encPriv, encPub []byte) error {
	kf := KeyFile{
		SignPublic:  EncodeKey(signPub),
		SignPrivate: EncodeKey(signPriv),
		EncPrivate:  EncodeKey(encPriv),
		EncPublic:   EncodeKey(encPub),
	}
	data, err := json.MarshalIndent(kf, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0600)
}

func GenerateSigningKey() (ed25519.PublicKey, ed25519.PrivateKey, error) {
	return ed25519.GenerateKey(rand.Reader)
}

func GenerateEncryptionKey() ([]byte, []byte, error) {
	priv := make([]byte, curve25519.ScalarSize)
	if _, err := io.ReadFull(rand.Reader, priv); err != nil {
		return nil, nil, err
	}
	priv[0] &= 248
	priv[31] &= 127
	priv[31] |= 64
	pub, err := curve25519.X25519(priv, curve25519.Basepoint)
	if err != nil {
		return nil, nil, err
	}
	return priv, pub, nil
}

func DeriveSharedKey(privateKey, publicKey []byte) ([]byte, error) {
	secret, err := curve25519.X25519(privateKey, publicKey)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(secret)
	return hash[:], nil
}

func EncryptAES(plaintext, aesKey []byte) ([]byte, []byte, error) {
	block, err := aes.NewCipher(aesKey)
	if err != nil {
		return nil, nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, nil, err
	}
	ciphertext := gcm.Seal(nil, nonce, plaintext, nil)
	return ciphertext, nonce, nil
}

func DecryptAES(ciphertext, nonce, aesKey []byte) ([]byte, error) {
	block, err := aes.NewCipher(aesKey)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return gcm.Open(nil, nonce, ciphertext, nil)
}

func Sign(privateKey ed25519.PrivateKey, data []byte) []byte {
	return ed25519.Sign(privateKey, data)
}

func Verify(publicKey ed25519.PublicKey, data, sig []byte) bool {
	return ed25519.Verify(publicKey, data, sig)
}

func EncodeKey(key []byte) string {
	return base64.StdEncoding.EncodeToString(key)
}

func DecodeKey(s string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(s)
}
