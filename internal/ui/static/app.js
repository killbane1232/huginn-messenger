let peers = [];
let activePeer = null;
let me = {};
let searchTimeout = null;

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
    const initials = name.charAt(0).toUpperCase();

    const div = document.createElement('div');
    div.className = 'peer-item' + (activePeer === p.id ? ' active' : '');
    div.innerHTML = `
      <div class="peer-avatar ${p.online ? 'online' : 'offline'}">${initials}</div>
      <div class="peer-info">
        <div class="peer-name">${name}</div>
        <div class="peer-status"><span class="${p.online ? 'status-online' : 'status-offline'}">${p.online ? 'online' : 'offline'}</span></div>
      </div>
    `;
    div.addEventListener('click', () => selectPeer(p.id));
    list.appendChild(div);
  });
}

async function selectPeer(peerID) {
  activePeer = peerID;
  showChat();
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

function renderFileLinks(files) {
  if (!files || files.length === 0) return '';
  return files.map(f =>
    '<div class="msg-file"><a href="/api/files/' + f.file_id + '" download target="_blank">&#128196; File (' + f.file_id.slice(0,8) + '...)</a></div>'
  ).join('');
}

function renderMessages(msgs, peerID) {
  const container = document.getElementById('messages');
  container.innerHTML = '';
  msgs.forEach(msg => {
    const isSent = msg.from === me.username;
    const div = document.createElement('div');
    div.className = 'msg ' + (isSent ? 'sent' : 'received');
    const time = new Date(msg.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    const senderLabel = isSent ? '' : '<div class="msg-sender">' + msg.from + '</div>';
    const fileLinks = msg.files ? renderFileLinks(msg.files) : '';
    div.innerHTML = senderLabel + msg.text + fileLinks + '<div class="msg-time">' + time + '</div>';
    container.appendChild(div);
  });
  container.scrollTop = container.scrollHeight;
}

async function sendMessage() {
  const input = document.getElementById('msg-input');
  const text = input.value.trim();
  if (!text || !activePeer) return;

  const ttlSelect = document.getElementById('ttl-select');
  const ttl = parseInt(ttlSelect.value) || 0;

  const fileInput = document.getElementById('file-input');
  const file = fileInput.files[0];

  if (file) {
    const formData = new FormData();
    formData.append('to', activePeer);
    formData.append('text', text);
    formData.append('ttl', ttl);
    formData.append('file', file);

    input.value = '';
    fileInput.value = '';
    document.getElementById('file-name').textContent = '';

    const r = await fetch('/api/send-file', {
      method: 'POST',
      body: formData
    });
    if (!r.ok) {
      const err = await r.text();
      alert('Send failed: ' + err);
      return;
    }
  } else {
    input.value = '';
    const r = await fetch('/api/send', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ to: activePeer, text, ttl })
    });
    if (!r.ok) {
      const err = await r.text();
      alert('Send failed: ' + err);
      return;
    }
  }

  const msgs = await fetchMessages(activePeer);
  renderMessages(msgs, activePeer);
}

function showChat() {
  document.getElementById('config-panel').style.display = 'none';
  document.getElementById('main').style.display = 'flex';
}

function showConfig() {
  document.getElementById('main').style.display = 'none';
  document.getElementById('no-chat').style.display = 'none';
  document.getElementById('config-panel').style.display = 'flex';
}

async function loadConfig() {
  const r = await fetch('/api/config');
  const cfg = await r.json();
  document.getElementById('cfg-username').value = cfg.username || '';
  document.getElementById('cfg-muninn').value = cfg.muninn || '';
  document.getElementById('cfg-ui-port').value = cfg.ui_port || 0;
  if (cfg.chunk_ttl) {
    document.getElementById('cfg-chunk-ttl').value = cfg.chunk_ttl;
  }
}

async function saveConfig() {
  const btn = document.getElementById('cfg-save');
  const status = document.getElementById('cfg-status');
  btn.disabled = true;
  status.textContent = '';

  const r = await fetch('/api/config', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      username: document.getElementById('cfg-username').value.trim(),
      muninn: document.getElementById('cfg-muninn').value.trim(),
      ui_port: parseInt(document.getElementById('cfg-ui-port').value) || 0,
      chunk_ttl: document.getElementById('cfg-chunk-ttl').value
    })
  });

  btn.disabled = false;

  if (r.ok) {
    status.textContent = 'Saved. Restart the app for changes to take effect.';
    status.className = 'cfg-ok';
  } else {
    const err = await r.text();
    status.textContent = 'Error: ' + err;
    status.className = 'cfg-err';
  }
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
    if (activePeer && (msg.from === activePeer || msg.from === me.username)) {
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
  document.getElementById('file-input').addEventListener('change', function(e) {
    const name = e.target.files[0] ? e.target.files[0].name : '';
    document.getElementById('file-name').textContent = name;
  });
  document.getElementById('peer-search').addEventListener('input', function() {
    clearTimeout(searchTimeout);
    const q = this.value.trim();
    searchTimeout = setTimeout(async () => {
      if (!q) {
        await fetchPeers();
        return;
      }
      const r = await fetch('/api/peers/search?q=' + encodeURIComponent(q));
      peers = await r.json();
      renderPeerList();
    }, 200);
  });

  document.getElementById('settings-btn').addEventListener('click', async function() {
    showConfig();
    await loadConfig();
  });
  document.getElementById('cfg-cancel').addEventListener('click', function() {
    if (activePeer) {
      selectPeer(activePeer);
    } else {
      showChat();
      document.getElementById('no-chat').style.display = 'flex';
    }
  });
  document.getElementById('cfg-save').addEventListener('click', saveConfig);
});
