let peers = [];
let activePeer = null;
let me = {};

async function fetchMe() {
  const r = await fetch('/api/me');
  me = await r.json();
}

async function fetchPeers() {
  const r = await fetch('/api/peers');
  peers = await r.json();
  renderPeerList();
}

async function fetchMessages(peerID) {
  const r = await fetch('/api/messages/' + encodeURIComponent(peerID));
  return await r.json();
}

function renderPeerList() {
  const list = document.getElementById('peer-list');
  list.innerHTML = '';
  peers.forEach(p => {
    const name = p.metadata && p.metadata.username ? p.metadata.username : p.id;
    const addr = p.addresses && p.addresses[0] ? p.addresses[0] : 'unknown';
    const initials = name.charAt(0).toUpperCase();

    const div = document.createElement('div');
    div.className = 'peer-item' + (activePeer === p.id ? ' active' : '');
    div.innerHTML = `
      <div class="peer-avatar">${initials}</div>
      <div class="peer-info">
        <div class="peer-name">${name}</div>
        <div class="peer-addr">${addr}</div>
      </div>
      <div class="peer-score">${p.quality_score || 0}</div>
    `;
    div.addEventListener('click', () => selectPeer(p.id));
    list.appendChild(div);
  });
}

async function selectPeer(peerID) {
  activePeer = peerID;
  renderPeerList();

  const p = peers.find(p => p.id === peerID);
  const name = p && p.metadata && p.metadata.username ? p.metadata.username : peerID;

  document.getElementById('no-chat').style.display = 'none';
  document.getElementById('main').style.display = 'flex';
  document.getElementById('chat-header').textContent = name;

  const msgs = await fetchMessages(peerID);
  renderMessages(msgs, peerID);
  document.getElementById('msg-input').disabled = false;
  document.getElementById('send-btn').disabled = false;
  document.getElementById('msg-input').focus();
}

function renderMessages(msgs, peerID) {
  const container = document.getElementById('messages');
  container.innerHTML = '';
  msgs.forEach(msg => {
    const isSent = msg.from === me.username;
    const div = document.createElement('div');
    div.className = 'msg ' + (isSent ? 'sent' : 'received');
    const time = new Date(msg.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    div.innerHTML = msg.text + '<div class="msg-time">' + time + '</div>';
    container.appendChild(div);
  });
  container.scrollTop = container.scrollHeight;
}

async function sendMessage() {
  const input = document.getElementById('msg-input');
  const text = input.value.trim();
  if (!text || !activePeer) return;

  input.value = '';
  const r = await fetch('/api/send', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ to: activePeer, text })
  });
  if (!r.ok) {
    const err = await r.text();
    alert('Send failed: ' + err);
    return;
  }
  const msgs = await fetchMessages(activePeer);
  renderMessages(msgs, activePeer);
}

function setupSSE() {
  const evtSource = new EventSource('/api/events');

  evtSource.addEventListener('peers', function(e) {
    peers = JSON.parse(e.data);
    renderPeerList();
    if (activePeer) {
      const stillExists = peers.some(p => p.id === activePeer);
      if (!stillExists) {
        activePeer = null;
        document.getElementById('main').style.display = 'none';
        document.getElementById('no-chat').style.display = 'flex';
      }
    }
  });

  evtSource.addEventListener('message', async function(e) {
    const msg = JSON.parse(e.data);
    if (activePeer && (msg.from === activePeer || msg.to === me.username)) {
      const msgs = await fetchMessages(activePeer);
      renderMessages(msgs, activePeer);
    }
  });

  evtSource.onerror = function() {
    console.error('SSE error, reconnecting...');
  };
}

document.addEventListener('DOMContentLoaded', async function() {
  await fetchMe();
  await fetchPeers();
  setupSSE();

  document.getElementById('send-btn').addEventListener('click', sendMessage);
  document.getElementById('msg-input').addEventListener('keydown', function(e) {
    if (e.key === 'Enter') sendMessage();
  });
});
