package chunk

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/killbane1232/huginn-messenger/internal/crypto"
)

const ChunkSize = 1024

type Envelope struct {
	MessageID    string `json:"message_id"`
	SenderID     string `json:"sender_id"`
	RecipientID  string `json:"recipient_id,omitempty"`
	TotalChunks  int    `json:"total_chunks"`
	ChunkIndex   int    `json:"chunk_index"`
	Ciphertext   string `json:"ciphertext"`
	Nonce        string `json:"nonce"`
	EphemeralKey string `json:"ephemeral_key,omitempty"`
	Signature    string `json:"signature"`
}

func SplitAndEncrypt(messageID, senderID, recipientID string, plaintext []byte, recipientPubKey []byte, signKey ed25519.PrivateKey) ([]Envelope, error) {
	ephemPriv, ephemPub, err := crypto.GenerateEncryptionKey()
	if err != nil {
		return nil, fmt.Errorf("generate ephemeral key: %w", err)
	}

	aesKey, err := crypto.DeriveSharedKey(ephemPriv, recipientPubKey)
	if err != nil {
		return nil, fmt.Errorf("derive shared key: %w", err)
	}

	total := (len(plaintext) + ChunkSize - 1) / ChunkSize
	envelopes := make([]Envelope, total)
	ephemKeyEncoded := crypto.EncodeKey(ephemPub)

	for i := 0; i < total; i++ {
		start := i * ChunkSize
		end := start + ChunkSize
		if end > len(plaintext) {
			end = len(plaintext)
		}

		ciphertext, nonce, err := crypto.EncryptAES(plaintext[start:end], aesKey)
		if err != nil {
			return nil, fmt.Errorf("encrypt chunk %d: %w", i, err)
		}

		env := Envelope{
			MessageID:    messageID,
			SenderID:     senderID,
			RecipientID:  recipientID,
			TotalChunks:  total,
			ChunkIndex:   i,
			Ciphertext:   base64.StdEncoding.EncodeToString(ciphertext),
			Nonce:        crypto.EncodeKey(nonce),
			EphemeralKey: ephemKeyEncoded,
		}

		sigData := envelopeBytes(env)
		sig := crypto.Sign(signKey, sigData)
		env.Signature = crypto.EncodeKey(sig)
		envelopes[i] = env
	}

	return envelopes, nil
}

func AssembleAndDecrypt(envelopes []Envelope, myPrivateKey, myPublicKey []byte, verifyKey ed25519.PublicKey) ([]byte, error) {
	if len(envelopes) == 0 {
		return nil, fmt.Errorf("no envelopes")
	}

	for _, env := range envelopes {
		sig, err := crypto.DecodeKey(env.Signature)
		if err != nil {
			return nil, fmt.Errorf("decode sig: %w", err)
		}
		sigData := envelopeBytes(env)
		if !crypto.Verify(verifyKey, sigData, sig) {
			return nil, fmt.Errorf("invalid signature on envelope")
		}
	}

	ephemPub, err := crypto.DecodeKey(envelopes[0].EphemeralKey)
	if err != nil {
		return nil, fmt.Errorf("decode ephemeral key: %w", err)
	}

	aesKey, err := crypto.DeriveSharedKey(myPrivateKey, ephemPub)
	if err != nil {
		return nil, fmt.Errorf("derive shared key: %w", err)
	}

	type chunkData struct {
		index int
		data  []byte
	}
	var chunks []chunkData

	for _, env := range envelopes {
		nonce, err := crypto.DecodeKey(env.Nonce)
		if err != nil {
			return nil, fmt.Errorf("decode nonce: %w", err)
		}
		ciphertext, err := base64.StdEncoding.DecodeString(env.Ciphertext)
		if err != nil {
			return nil, fmt.Errorf("decode ciphertext: %w", err)
		}

		plaintext, err := crypto.DecryptAES(ciphertext, nonce, aesKey)
		if err != nil {
			return nil, fmt.Errorf("decrypt chunk %d: %w", env.ChunkIndex, err)
		}
		chunks = append(chunks, chunkData{index: env.ChunkIndex, data: plaintext})
	}

	sort.Slice(chunks, func(i, j int) bool { return chunks[i].index < chunks[j].index })

	var buf bytes.Buffer
	for _, c := range chunks {
		buf.Write(c.data)
	}

	return buf.Bytes(), nil
}

func SplitAndEncryptFile(fileID, senderID string, plaintext []byte, aesKey []byte, signKey ed25519.PrivateKey) ([]Envelope, error) {
	total := (len(plaintext) + ChunkSize - 1) / ChunkSize
	envelopes := make([]Envelope, total)

	for i := 0; i < total; i++ {
		start := i * ChunkSize
		end := start + ChunkSize
		if end > len(plaintext) {
			end = len(plaintext)
		}

		ciphertext, nonce, err := crypto.EncryptAES(plaintext[start:end], aesKey)
		if err != nil {
			return nil, fmt.Errorf("encrypt chunk %d: %w", i, err)
		}

		env := Envelope{
			MessageID:   fileID,
			SenderID:    senderID,
			TotalChunks: total,
			ChunkIndex:  i,
			Ciphertext:  base64.StdEncoding.EncodeToString(ciphertext),
			Nonce:       crypto.EncodeKey(nonce),
		}

		sigData := envelopeBytes(env)
		sig := crypto.Sign(signKey, sigData)
		env.Signature = crypto.EncodeKey(sig)
		envelopes[i] = env
	}

	return envelopes, nil
}

func AssembleAndDecryptFile(envelopes []Envelope, aesKey []byte, verifyKey ed25519.PublicKey) ([]byte, error) {
	if len(envelopes) == 0 {
		return nil, fmt.Errorf("no envelopes")
	}

	for _, env := range envelopes {
		sig, err := crypto.DecodeKey(env.Signature)
		if err != nil {
			return nil, fmt.Errorf("decode sig: %w", err)
		}
		sigData := envelopeBytes(env)
		if !crypto.Verify(verifyKey, sigData, sig) {
			return nil, fmt.Errorf("invalid signature on envelope")
		}
	}

	type chunkData struct {
		index int
		data  []byte
	}
	var chunks []chunkData

	for _, env := range envelopes {
		nonce, err := crypto.DecodeKey(env.Nonce)
		if err != nil {
			return nil, fmt.Errorf("decode nonce: %w", err)
		}
		ciphertext, err := base64.StdEncoding.DecodeString(env.Ciphertext)
		if err != nil {
			return nil, fmt.Errorf("decode ciphertext: %w", err)
		}

		plaintext, err := crypto.DecryptAES(ciphertext, nonce, aesKey)
		if err != nil {
			return nil, fmt.Errorf("decrypt chunk %d: %w", env.ChunkIndex, err)
		}
		chunks = append(chunks, chunkData{index: env.ChunkIndex, data: plaintext})
	}

	sort.Slice(chunks, func(i, j int) bool { return chunks[i].index < chunks[j].index })

	var buf bytes.Buffer
	for _, c := range chunks {
		buf.Write(c.data)
	}

	return buf.Bytes(), nil
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
		"message_id":   env.MessageID,
		"sender_id":    env.SenderID,
		"total_chunks": fmt.Sprintf("%d", env.TotalChunks),
		"chunk_index":  fmt.Sprintf("%d", env.ChunkIndex),
		"ciphertext":   env.Ciphertext,
		"nonce":        env.Nonce,
	}
	if env.RecipientID != "" {
		data["recipient_id"] = env.RecipientID
	}
	if env.EphemeralKey != "" {
		data["ephemeral_key"] = env.EphemeralKey
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
