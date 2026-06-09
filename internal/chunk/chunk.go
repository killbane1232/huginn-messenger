package chunk

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/killbane1232/huginn-messenger/internal/crypto"
)

const ChunkSize = 1024

type Envelope struct {
	MessageID    string `json:"message_id"`
	SenderID     string `json:"sender_id"`
	RecipientID  string `json:"recipient_id"`
	TotalChunks  int    `json:"total_chunks"`
	ChunkIndex   int    `json:"chunk_index"`
	EncryptedKey string `json:"encrypted_key"`
	Ciphertext   string `json:"ciphertext"`
	Nonce        string `json:"nonce"`
	FullNonce    string `json:"full_nonce"`
	Signature    string `json:"signature"`
}

func SplitAndEncrypt(messageID, senderID, recipientID string, plaintext []byte, aesKey []byte, signKey ed25519.PrivateKey) ([]Envelope, error) {
	encryptedKey := crypto.EncodeKey(aesKey)

	ciphertextFull, fullNonce, err := crypto.EncryptAES(plaintext, aesKey)
	if err != nil {
		return nil, fmt.Errorf("encrypt full text: %w", err)
	}

	chunkSize := ChunkSize
	total := (len(ciphertextFull) + chunkSize - 1) / chunkSize
	envelopes := make([]Envelope, total)

	for i := 0; i < total; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if end > len(ciphertextFull) {
			end = len(ciphertextFull)
		}

		ciphertext, nonce, err := crypto.EncryptAES(ciphertextFull[start:end], aesKey)
		if err != nil {
			return nil, fmt.Errorf("encrypt chunk %d: %w", i, err)
		}

		env := Envelope{
			MessageID:    messageID,
			SenderID:     senderID,
			RecipientID:  recipientID,
			TotalChunks:  total,
			ChunkIndex:   i,
			EncryptedKey: encryptedKey,
			Ciphertext:   crypto.EncodeKey(ciphertext),
			Nonce:        crypto.EncodeKey(nonce),
			FullNonce:    crypto.EncodeKey(fullNonce),
		}

		sigData := envelopeBytes(env)
		sig := crypto.Sign(signKey, sigData)
		env.Signature = crypto.EncodeKey(sig)
		envelopes[i] = env
	}

	return envelopes, nil
}

func AssembleAndDecrypt(envelopes []Envelope, decryptKey []byte, verifyKey ed25519.PublicKey) ([]byte, error) {
	if len(envelopes) == 0 {
		return nil, fmt.Errorf("no envelopes")
	}

	for _, env := range envelopes {
		sig, err := crypto.DecodeKey(env.Signature)
		if err != nil {
			return nil, fmt.Errorf("decode sig chunk %d: %w", env.ChunkIndex, err)
		}
		sigData := envelopeBytes(env)
		if !crypto.Verify(verifyKey, sigData, sig) {
			return nil, fmt.Errorf("invalid signature on chunk %d", env.ChunkIndex)
		}
	}

	var aesKey []byte
	encKey, err := crypto.DecodeKey(envelopes[0].EncryptedKey)
	if err != nil {
		return nil, fmt.Errorf("decode encrypted key: %w", err)
	}
	if bytes.Equal(encKey, decryptKey) {
		aesKey = decryptKey
	} else {
		aesKey = encKey
	}

	fullNonce, err := crypto.DecodeKey(envelopes[0].FullNonce)
	if err != nil {
		return nil, fmt.Errorf("decode full nonce: %w", err)
	}

	var outerBuf bytes.Buffer
	for _, env := range envelopes {
		ct, err := crypto.DecodeKey(env.Ciphertext)
		if err != nil {
			return nil, fmt.Errorf("decode ct chunk %d: %w", env.ChunkIndex, err)
		}
		innerNonce, err := crypto.DecodeKey(env.Nonce)
		if err != nil {
			return nil, fmt.Errorf("decode nonce chunk %d: %w", env.ChunkIndex, err)
		}
		chunkPlain, err := crypto.DecryptAES(ct, innerNonce, aesKey)
		if err != nil {
			return nil, fmt.Errorf("decrypt inner chunk %d: %w", env.ChunkIndex, err)
		}
		outerBuf.Write(chunkPlain)
	}

	plaintext, err := crypto.DecryptAES(outerBuf.Bytes(), fullNonce, aesKey)
	if err != nil {
		return nil, fmt.Errorf("decrypt outer: %w", err)
	}

	return plaintext, nil
}

func ComputeHash(data []byte) string {
	h := sha256.Sum256(data)
	return base64.StdEncoding.EncodeToString(h[:])
}

func RegisteredHash(data []byte) string {
	h := sha256.Sum256(data)
	return fmt.Sprintf("%x", h[:8])
}

func envelopeBytes(env Envelope) []byte {
	data := map[string]string{
		"message_id":    env.MessageID,
		"sender_id":     env.SenderID,
		"recipient_id":  env.RecipientID,
		"chunk_index":   fmt.Sprintf("%d", env.ChunkIndex),
		"total_chunks":  fmt.Sprintf("%d", env.TotalChunks),
		"encrypted_key": env.EncryptedKey,
		"ciphertext":    env.Ciphertext,
		"nonce":         env.Nonce,
		"full_nonce":    env.FullNonce,
	}
	b, _ := json.Marshal(data)
	return b
}

func MarshalEnvelope(env Envelope) ([]byte, error) {
	return json.Marshal(env)
}

func UnmarshalEnvelope(data []byte) (Envelope, error) {
	var env Envelope
	err := json.Unmarshal(data, &env)
	return env, err
}
