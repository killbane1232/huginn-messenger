# Huginn Messenger

Desktop P2P messenger using [Muninn](https://github.com/killbane1232/muninn) for peer discovery, with end-to-end encryption, WebRTC, and offline chunked delivery.

## Features

- **E2E Encryption** — AES-256-GCM with X25519 ECDH key exchange, Ed25519 signing
- **WebRTC** — direct P2P data channel when both peers are online
- **Offline delivery** — when recipient is offline, messages are split into chunks and distributed to quality-ranked peers
- **Chunk verification** — recipient verifies hashes and reports to Muninn for quality scoring
- **Peer discovery** — via Muninn phonebook server
- **Real-time UI** — web interface with SSE updates

## Architecture

```
              ┌─────────────┐
              │   Muninn    │  phonebook + chunk registry
              │  :8080      │
              └──────┬──────┘
                     │ REST API
        ┌────────────┼────────────┐
        │            │            │
   ┌────▼────┐  ┌────▼────┐  ┌────▼────┐
   │  Alice  │  │   Bob   │  │Storage  │
   │messenger│  │messenger│  │ Peers   │
   └─────────┘  └─────────┘  └─────────┘
        │            │
        └────P2P─────┘  HTTP / WebRTC
```

## Usage

### 1. Start Muninn server (modified version with chunk recipient tracking)

```bash
cd /path/to/muninn
go run ./cmd/server
```

### 2. Build & run the messenger

```bash
cd huginn-messenger
go build -o huginn-messenger .

# Terminal 1 — Alice
./huginn-messenger --username alice --muninn http://localhost:8080

# Terminal 2 — Bob
./huginn-messenger --username bob --muninn http://localhost:8080
```

### 3. Open the web UI

Each instance prints a URL like `http://localhost:XXXXX`. Open it in your browser.

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--username` | (required) | Your username |
| `--muninn` | `http://localhost:8080` | Muninn server address |
| `--ui-port` | random | Web UI port |
| `--msg-port` | random | P2P message port |

## How it works

### Online messaging
1. Both peers register with Muninn (Ed25519 signing key + X25519 encryption key)
2. Messages are encrypted with AES-256-GCM (ECDH-derived key), signed with Ed25519
3. Sent via direct HTTP POST or WebRTC data channel
4. Recipient decrypts and verifies the signature

### Offline messaging
1. Sender detects recipient is offline (HTTP connection refused)
2. Message is split into 1KB chunks
3. Each chunk is encrypted + signed individually
4. Top-N best peers (by quality score) are selected from Muninn
5. Each chunk is stored on a different best peer
6. Chunk hashes are registered in Muninn with `recipient_id`

### Message discovery (coming online)
1. Recipient queries Muninn: `GET /api/v1/recipient/{id}/chunks`
2. Muninn returns list of `(file_id, chunk_index, hash, peer_id)`
3. Recipient fetches chunks from storage peers
4. Verifies hashes, decrypts, reconstructs message
5. Reports each chunk to Muninn for quality scoring
6. Verified chunks are deleted from storage peers

## Project structure

```
main.go                        — entry point
internal/
  config/config.go             — command-line flags
  crypto/crypto.go             — AES-256-GCM, X25519 ECDH, Ed25519 sign/verify
  chunk/chunk.go               — message chunking, encryption, reassembly
  webrtc/webrtc.go             — WebRTC peer connection manager (Pion)
  muninn/client.go             — Muninn REST API client
  p2p/p2p.go                   — P2P transport (HTTP) + chunk storage
  messenger/messenger.go       — core logic (send/receive/offline/collect)
  ui/server.go                 — web UI server + SSE
  ui/static/                   — embedded HTML/CSS/JS
```
