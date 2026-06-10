# Huginn Messenger — архитектура передачи сообщений

## 1. Регистрация пира и heartbeat

```mermaid
sequenceDiagram
    participant A as Alice (huginn)
    participant M as Muninn Server
    participant B as Bob (huginn)

    A->>M: POST /api/v1/peers (Register)
    M-->>A: 201 Created
    Note over A: keys.conf загружены/сгенерированы
    Note over A: свой ID и username = "alice"

    loop Каждые 15s
        A->>M: POST /api/v1/peers/alice/heartbeat
        M-->>A: 200 OK
    end

    B->>M: POST /api/v1/peers (Register)
    M-->>B: 201 Created
```

Каждый экземпляр Huginn при старте регистрируется на Muninn-сервере, передавая свои публичные ключи (encryption + signing), метаданные и TTL (120s). Каждые 15 секунд отправляется heartbeat, продлевающий регистрацию. Если heartbeat не пришёл вовремя — пир считается офлайн.

---

## 2. Поиск пиров

```mermaid
sequenceDiagram
    participant UI as Browser (app.js)
    participant API as Go HTTP Server
    participant DB as SQLite (stored_peers)
    participant M as Muninn Server

    UI->>API: GET /api/peers/search?q=alice
    API->>DB: SELECT FROM stored_peers WHERE peer_id LIKE '%alice%'
    DB-->>API: [StoredPeer{peer_id:"alice", ...}]
    API->>M: GET /api/v1/peers (List all)
    M-->>API: [Peer{id:"alice", ...}, Peer{id:"bob", ...}]
    Note over API: merge по peer_id,<br/>при дубляже Muninn побеждает
    API-->>UI: [{id:"alice", online:true, ...}]
```

Поиск работает в два слоя: сначала SQLite (`stored_peers` — пиры, с которыми уже было взаимодействие), затем Muninn (все зарегистрированные пиры). Результаты мержатся по `peer_id`.

---

## 3. WebRTC Signaling (установка P2P-канала)

WebRTC-соединение устанавливается через сигнальный обмен offer/answer. Есть два механизма: новый — через постоянное WebRTC-соединение с Muninn (рекомендуемый), и старый — через HTTP polling (fallback).

### 3a. WebRTC-to-Muninn (новый, основной)

```mermaid
sequenceDiagram
    participant A as Alice (huginn)
    participant MA as Muninn (WebRTC)
    participant MB as Muninn (WebRTC)
    participant B as Bob (huginn)

    Note over A: Bootstrap WebRTC к Muninn
    A->>MA: POST /api/v1/webrtc/bootstrap (SDP offer)
    MA-->>A: SDP answer
    Note over A,MA: DataChannel "muninn-rpc" установлен

    Note over B: Bootstrap WebRTC к Muninn
    B->>MB: POST /api/v1/webrtc/bootstrap (SDP offer)
    MB-->>B: SDP answer
    Note over B,MB: DataChannel "muninn-rpc" установлен

    Note over A: Alice хочет соединиться с Bob
    A->>A: CreateOffer() → pion.SessionDescription
    A->>MA: RPC "connect_to_peer" {target:"bob", offer:"..."}
    MA->>MB: RPC notify "incoming_signal" {from:"alice", type:"offer", data:"..."}
    MB->>B: RPC notify "incoming_signal"
    B->>B: HandleOffer() → CreateAnswer()
    B->>MB: RPC "signal_relay" {target:"alice", type:"answer", data:"..."}
    MB->>MA: RPC notify "incoming_signal" {from:"bob", type:"answer", data:"..."}
    MA->>A: RPC notify "incoming_signal"
    A->>A: SetRemoteDescription(answer)
    Note over A,B: P2P WebRTC DataChannel установлен
```

Клиент при старте устанавливает постоянное WebRTC-соединение с сервером Muninn (bootstrap через одноразовый HTTP-запрос). Все последующие сигналы обмена offer/answer передаются мгновенно через это соединение в виде RPC-сообщений, без HTTP polling.

### 3b. HTTP Polling (старый, fallback)

```mermaid
sequenceDiagram
    participant A as Alice
    participant M as Muninn (HTTP)
    participant B as Bob

    Note over A: Alice хочет отправить<br/>сообщение Bob (он онлайн)
    A->>A: CreateOffer() → pion.SessionDescription
    A->>M: POST /api/v1/peers/bob/signals {from:"alice", type:"offer", data:"..."}
    Note over M: сигнал хранится в очереди Bob
    loop Polling каждые 500ms
        B->>M: GET /api/v1/peers/bob/signals
        M-->>B: [{from:"alice", type:"offer", data:"..."}]
    end
    B->>B: HandleOffer() → CreateAnswer()
    B->>M: POST /api/v1/peers/alice/signals {from:"bob", type:"answer", data:"..."}
    Note over M: сигнал хранится в очереди Alice
    loop Polling каждые 500ms
        A->>M: GET /api/v1/peers/alice/signals
        M-->>A: [{from:"bob", type:"answer", data:"..."}]
    end
    A->>A: SetRemoteDescription(answer)
    Note over A,B: WebRTC DataChannel установлен
```

Если WebRTC-соединение с Muninn недоступно, клиент автоматически переключается на HTTP polling (каждые 500ms) для обмена сигналами. Сервер Muninn при получении RPC-сигнала для пира, не подключённого через WebRTC, сохраняет сигнал в Store — пир заберёт его через HTTP.

---

## 4. Онлайн-доставка (WebRTC)

```mermaid
sequenceDiagram
    participant A as Alice
    participant DC as WebRTC DataChannel
    participant B as Bob
    participant A_DB as Alice SQLite
    participant B_DB as Bob SQLite

    Note over A,B: DataChannel уже открыт
    A->>A_DB: SaveMessage(chat_id=bob, ...)
    A->>DC: send({type:"chat", from:"alice", text:"hello", ...})
    DC-->>B: message received
    B->>B_DB: SaveMessage(chat_id=alice, ...)
    B->>B_DB: StorePeer(alice, enc_key, sign_key)
    Note over B: trigger SSE event "message"
    B-->>A: (no ack — fire-and-forget)
```

Если WebRTC-канал открыт, сообщение отправляется напрямую через DataChannel. Отправитель сохраняет сообщение у себя в БД, получатель — у себя. Никаких подтверждений доставки не предусмотрено (fire-and-forget).

---

## 5. Офлайн-доставка (Chunks)

```mermaid
sequenceDiagram
    participant A as Alice (sender)
    participant A_DB as Alice SQLite
    participant M as Muninn
    participant SP as Storage Peers
    participant B as Bob (recipient)
    participant B_DB as Bob SQLite

    Note over A: Bob офлайн или канал не открылся
    A->>A: SplitAndEncrypt(msg) → []Envelope
    Note over A: каждая часть 16 байт (1 блок AES),<br/>AES-256-GCM + Ed25519 signature

    loop For each envelope
        A->>A_DB: StoreChunk(msgID, index, data)
    end

    A->>M: GET /api/v1/peers/best?n=10
    M-->>A: [Peer{charley}, Peer{dave}, ...]

    Note over A: подключение к storage peers
    loop For each connected storage peer
        A->>SP: SendChunkStoreBatch (WebRTC batch)
        SP-->>SP: StoreChunk(msgID, index, data)
    end

    A->>M: POST /api/v1/alice/chunks (RegisterChunks batch)
    Note over M: ChunkRecord{fileID, chunkIndex,<br/>sender, recipient, hash, sig, peerID}

    A->>A_DB: StorePendingChunk(placed=true/false)
    Note over A: placed=true если хотя бы<br/>один storage peer получил чанк

    A->>A_DB: SaveMessage + StorePeer

    Note over B: позже, Bob заходит онлайн
    B->>M: GET /api/v1/recipient/bob/chunks
    M-->>B: [ChunkRecord{fileID, peerID, ...}, ...]

    Note over B: сборка чанков от разных storage peers
    B->>SP: SendChunkGet (WebRTC)
    SP-->>B: ChunkData

    B->>B: AssembleAndDecrypt(envelopes) → plaintext
    B->>B_DB: SaveMessage
    B->>M: DELETE /api/v1/recipient/bob/chunks/{msgID}
```

Если P2P-канал не открылся (пир офлайн), сообщение разбивается на 1KB-зашифрованные чанки. Каждый чанк сохраняется локально и реплицируется на соседние онлайн-пиры (storage peers). Метаданные о местоположении чанков регистрируются на Muninn. Получатель периодически опрашивает Muninn о новых чанках для себя, собирает их со storage peers и дешифрует.

---

## 6. Фоновая репликация чанков

```mermaid
flowchart LR
    subgraph "Каждые 15s (peerRefreshLoop)"
        A[checkPendingMessages] --> B[poll Muninn chunks]
        B --> C[collectAndProcessMessage]

        D[replicatePendingChunks] --> E[list chunk files from SQLite]
        E --> F{connected peers exist?}
        F -->|yes| G[SendChunkStoreBatch per peer]
        F -->|no| H[skip]

        I[processPendingSignals] --> J[poll Muninn signals]
    end
```

В цикле `peerRefreshLoop` (15s) выполняются три задачи:
- **checkPendingMessages** — опрос Muninn на предмет новых чанков, адресованных нам
- **replicatePendingChunks** — распространение локально хранящихся чанков на подключённых пиров
- **processPendingSignals** — обработка WebRTC-сигналов (offer/answer)

---

## 7. Фоновая отправка неразмещённых чанков

```mermaid
flowchart TB
    subgraph "Каждые 30s (pendingChunkLoop)"
        A[GetUnplacedChunks from SQLite] --> B{есть неразмещённые?}
        B -->|no| C[return]
        B -->|yes| D[Group by recipientID]

        D --> E[For each recipient]
        E --> F[GetBestPeers from Muninn]
        F --> G[Connect to storage peers]
        G --> H[Round-robin: chunk i → peer i % M]
        H --> I[SendChunkStoreBatch per peer]
        I --> J[RegisterChunks per file per peer]
        J --> K[MarkChunkPlaced in SQLite]
    end
```

Отдельная горутина `pendingChunkLoop` (30s) обрабатывает чанки, которые не удалось разместить при первой отправке (`placed=false`). Чанки группируются по получателю, затем распределяются по доступным storage peers по кругу (round-robin), отправляются батчами через WebRTC и регистрируются на Muninn.

---

## 8. Жизненный цикл pending_chunk

```mermaid
stateDiagram-v2
    [*] --> Created: sendOffline
    Created --> Placed: SendChunkStoreBatch успешен
    Created --> Pending: нет доступных пиров
    Pending --> Placed: pendingChunkLoop разместил
    Placed --> [*]: получатель подтвердил доставку<br/>(ещё не реализовано)

    note right of Pending
        Хранится в SQLite pending_chunks,
        placed=false, пока не разместится
        на хотя бы одном storage peer
    end note
```

Чанк создаётся в `sendOffline`. Если хотя бы один storage peer его получил — `placed=true`. Если нет — `placed=false`, и фоновый процесс будет пытаться разместить его навсегда (пока не появится механизм подтверждения доставки).

---

## 9. SSE-события (real-time UI)

```mermaid
sequenceDiagram
    participant UI as Browser
    participant S as Go HTTP Server (SSE)
    participant M as Messenger
    participant DB as SQLite

    UI->>S: GET /api/events (SSE)
    Note over S: SubscribePeers + SubscribeMessages

    alt Новый пир
        M->>M: сигнал/рефреш пиров
        M->>S: peerCh chan struct{}
        S->>M: GetPeers()
        M->>DB: (peers in memory)
        S-->>UI: event: peers\n[{id, online, ...}]
    end

    alt Новое сообщение
        M->>S: msgCh chan ChatMessage
        S-->>UI: event: message\n{from, text, timestamp}
        UI->>UI: fetchMessages(activePeer) → re-render
    end

    Note over S: keepalive каждые 10s
```

---

## 10. Пользовательский поиск (UI → API)

```mermaid
sequenceDiagram
    participant U as User
    participant UI as Browser
    participant API as Go Server
    participant DB as SQLite
    participant M as Muninn

    U->>UI: ввод "alice" в search
    UI->>UI: debounce 200ms
    UI->>API: GET /api/peers/search?q=alice

    API->>DB: SearchStoredPeers("alice")
    DB-->>API: [{peer_id:"alice", ...}]

    API->>M: GET /api/v1/peers
    M-->>API: [{id:"alice", ...}, {id:"bob", ...}]

    Note over API: merge + enrich online status
    API-->>UI: [{id:"alice", online:true, ...}]

    UI->>UI: renderPeerList()
```

Поиск на фронтенде с дебаунсом (200ms). При пустом запросе — возвращается полный список через стандартный `/api/peers`. При непустом — `/api/peers/search?q=...` с поиском по локальному SQLite + Muninn.

---

## 11. WebRTC RPC Protocol (клиент-серверный канал с Muninn)

Для замены HTTP polling сигналов используется постоянное WebRTC-соединение между каждым клиентом Huginn и сервером Muninn. Поверх DataChannel работает RPC-протокол.

### Bootstrap (HTTP → WebRTC)

```
POST /api/v1/webrtc/bootstrap
Headers: X-Peer-ID: <peer_id>
Body: pion.SessionDescription (SDP offer)
Response: pion.SessionDescription (SDP answer)
```

Одноразовый HTTP-запрос для начального handshake. Клиент создаёт `PeerConnection` и DataChannel `"muninn-rpc"`, отправляет SDP offer. Сервер создаёт answer. После этого всё общение идёт через WebRTC DataChannel.

### Протокол сообщений (DataChannel)

Все сообщения — JSON. Есть три типа:

**Request** (клиент → сервер):
```json
{"id": "uuid", "method": "method_name", "params": {...}}
```

**Response** (сервер → клиент):
```json
{"id": "uuid", "result": {...}, "error": ""}
```

**Notification** (сервер → клиент):
```json
{"method": "method_name", "params": {...}}
```

### RPC-методы

| Метод | Направление | Описание |
|-------|-------------|----------|
| `connect_to_peer` | client → server | Запрос на соединение с другим пиром. Server проверяет, подключён ли target через WebRTC; если да — шлёт notification, если нет — сохраняет сигнал в Store |
| `signal_relay` | client → server | Релей сигнала (offer/answer) целевому пиру |
| `incoming_signal` | server → client (notify) | Входящий сигнал от другого пира |

### Параметры методов

**connect_to_peer:**
```json
{"target_id": "bob", "offer": "SDP_offer_string"}
```

**signal_relay:**
```json
{"target_id": "bob", "from": "alice", "type": "answer", "data": "SDP_answer_string"}
```

**incoming_signal (notification):**
```json
{"from": "alice", "type": "offer", "data": "SDP_offer_string"}
```

### Обработка на сервере (Muninn)

Сервер (`internal/webrtc/handler.go`) поддерживает `map[string]*peerConn` — активные WebRTC-подключения пиров.

При получении `connect_to_peer` или `signal_relay`:
1. Проверить, есть ли target в `peers` (подключён через WebRTC)
2. Если да — отправить notification напрямую через DataChannel target'а
3. Если нет — сохранить сигнал в `store.Store.SetSignal()`, откуда target заберёт его через HTTP polling

При отключении пира — автоматический cleanup из map.

### Клиентская часть (Huginn)

Клиент (`internal/muninn/rtc.go` → `RTCClient`):
- Управляет `PeerConnection` к Muninn
- Отправляет RPC-запросы и сопоставляет ответы по UUID
- Принимает notification'ы через колбэк `OnSignal`
- Автоматический reconnect при обрыве (каждые 5s, в `rtcReconnectLoop`)
- Fallback на HTTP polling, если WebRTC недоступен

---

## Сводка протоколов обмена

| Сценарий | Протокол | Частота | Размер данных |
|----------|----------|---------|---------------|
| Регистрация | HTTP REST (Muninn) | При старте | ~500 bytes |
| Heartbeat | HTTP REST (Muninn) | Каждые 15s | ~50 bytes |
| Пинг сигналов | **WebRTC RPC** (Muninn DataChannel) | Push (мгновенно) | ~100 bytes |
| Пинг сигналов (fallback) | HTTP REST (Muninn) | Каждые 500ms | ~100 bytes |
| Bootstrap WebRTC-to-Muninn | HTTP REST (однократно) | При старте | ~2-5KB |
| Поиск пиров | HTTP REST (Muninn) | По запросу | зависит от N пиров |
| WebRTC Offer/Answer | **WebRTC RPC** (Muninn DataChannel) | Однократно при коннекте | ~2-5KB |
| WebRTC Offer/Answer (fallback) | HTTP REST (Muninn signals) | Однократно при коннекте | ~2-5KB |
| Онлайн-сообщение | WebRTC DataChannel (P2P) | Однократно | произвольный |
| Офлайн-чанк | WebRTC DataChannel (batch) | ~ раз в 30s | ~1KB × N чанков |
| Регистрация чанков | HTTP REST (Muninn, batch) | При отправке | ~200 bytes × N |
| Репликация чанков | WebRTC DataChannel (batch) | Каждые 15s | ~1KB × N |
| SSE события | HTTP Server-Sent Events | Постоянно | ~1-5KB |
| Поиск пользователей | HTTP REST (local API) | При вводе | ~100-500 bytes |
