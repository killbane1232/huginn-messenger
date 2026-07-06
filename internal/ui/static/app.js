let peers = [];
let groups = [];
let activePeer = null;
let activeGroup = null;
let me = {};
let searchTimeout = null;
let pendingFiles = [];
let lastFileAddTime = 0;

async function fetchMe() {
  try {
    const r = await fetch('/api/me');
    if (r.ok) me = await r.json();
  } catch (e) {}
}

async function fetchPeers() {
  try {
    const r = await fetch('/api/peers');
    if (!r.ok) { peers = []; renderPeerList(); return; }
    peers = await r.json();
  } catch (e) {
    peers = [];
  }
  renderPeerList();
}

async function fetchGroups() {
  try {
    const r = await fetch('/api/groups');
    if (!r.ok) { groups = []; renderGroupList(); return; }
    groups = await r.json();
  } catch (e) {
    groups = [];
  }
  renderGroupList();
}

async function fetchMessages(peerID) {
  const r = await fetch('/api/messages/' + encodeURIComponent(peerID));
  return await r.json();
}

function renderGroupList() {
  const list = document.getElementById('group-list');
  list.innerHTML = '';
  if (groups == null) {
    return
  }
  groups.forEach(g => {
    const div = document.createElement('div');
    div.className = 'peer-item' + (activeGroup === g.uid ? ' active' : '');
    div.innerHTML = `
      <div class="peer-avatar group-avatar">G</div>
      <div class="peer-info">
        <div class="peer-name">${g.name}</div>
        <div class="peer-status">group</div>
      </div>
    `;
    div.addEventListener('click', () => selectGroup(g.uid));
    list.appendChild(div);
  });
}

function renderPeerList() {
  const list = document.getElementById('peer-list');
  list.innerHTML = '';
  peers.forEach(p => {
    if (p.metadata && p.metadata.type === 'huginn-group') return;
    const name = p.metadata && p.metadata.username ? p.metadata.username : p.key.split(':')[0];
    const initials = name.charAt(0).toUpperCase();

    const div = document.createElement('div');
    div.className = 'peer-item' + (activePeer === p.key ? ' active' : '');
    div.innerHTML = `
      <div class="peer-avatar ${p.online ? 'online' : 'offline'}">${initials}</div>
      <div class="peer-info">
        <div class="peer-name">${name}</div>
        <div class="peer-status"><span class="${p.online ? 'status-online' : 'status-offline'}">${p.online ? 'online' : 'offline'}</span></div>
      </div>
    `;
    div.addEventListener('click', () => selectPeer(p.key));
    list.appendChild(div);
  });
}

function selectGroup(groupUID) {
  activeGroup = groupUID;
  activePeer = null;
  showChat();
  renderGroupList();
  renderPeerList();

  const g = groups.find(g => g.uid === groupUID);
  const name = g ? g.name : groupUID;

  document.getElementById('no-chat').style.display = 'none';
  document.getElementById('main').style.display = 'flex';
  document.getElementById('chat-header').textContent = name + ' (group)';
  document.getElementById('invite-area').style.display = 'flex';

  fetchMessages(groupUID).then(msgs => {
    renderMessages(msgs, groupUID);
  });
  document.getElementById('msg-input').disabled = false;
  document.getElementById('send-btn').disabled = false;
  document.getElementById('msg-input').focus();
}

async function selectPeer(peerID) {
  activePeer = peerID;
  activeGroup = null;
  showChat();
  renderPeerList();
  renderGroupList();

  const p = peers.find(p => p.key === peerID);
  const name = p && p.metadata && p.metadata.username ? p.metadata.username : peerID.split(':')[0];

  document.getElementById('no-chat').style.display = 'none';
  document.getElementById('main').style.display = 'flex';
  document.getElementById('chat-header').textContent = name;
  document.getElementById('invite-area').style.display = 'none';

  const msgs = await fetchMessages(peerID);
  renderMessages(msgs, peerID);
  document.getElementById('msg-input').disabled = false;
  document.getElementById('send-btn').disabled = false;
  document.getElementById('msg-input').focus();
}

function renderFileLinks(files) {
  if (!files || files.length === 0) return '';
  return files.map(f =>
    '<div class="msg-file"><a href="/api/files/' + f.file_id + '" download target="_blank">&#128196; ' + (f.filename || f.file_id.slice(0,8) + '...') + '</a></div>'
  ).join('');
}

function renderMessages(msgs, peerID) {
  const container = document.getElementById('messages');
  container.innerHTML = '';
  msgs.forEach(msg => {
    const isSent = msg.from === me.username;
    const isGroup = activeGroup !== null;
    const div = document.createElement('div');
    div.className = 'msg ' + (isSent ? 'sent' : 'received');
    const time = new Date(msg.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    const senderLabel = (isGroup && !isSent) ? '<div class="msg-sender">' + msg.from + '</div>' : '';
    const fileLinks = msg.files ? renderFileLinks(msg.files) : '';
    div.innerHTML = senderLabel + msg.text + fileLinks + '<div class="msg-time">' + time + '</div>';
    container.appendChild(div);
  });
  container.scrollTop = container.scrollHeight;
}

function renderPendingFiles() {
  const container = document.getElementById('file-preview');
  if (pendingFiles.length === 0) {
    container.style.display = 'none';
    container.innerHTML = '';
    return;
  }
  container.style.display = 'flex';
  container.innerHTML = pendingFiles.map((f, i) =>
    '<div class="pending-file">' +
      '<span class="pending-file-name">' + f.name + '</span>' +
      '<span class="pending-file-size">' + (f.size / 1024).toFixed(1) + ' KB</span>' +
      '<button class="pending-file-remove" data-index="' + i + '">&times;</button>' +
    '</div>'
  ).join('');
  container.querySelectorAll('.pending-file-remove').forEach(btn => {
    btn.addEventListener('click', function() {
      const idx = parseInt(this.dataset.index);
      pendingFiles.splice(idx, 1);
      renderPendingFiles();
    });
  });
}

function clearPendingFiles() {
  pendingFiles = [];
  document.getElementById('file-input').value = '';
  renderPendingFiles();
}

async function sendMessage() {
  const input = document.getElementById('msg-input');
  const text = input.value.trim();
  if (!text && pendingFiles.length === 0) return;

  if (!text && pendingFiles.length > 0 && Date.now() - lastFileAddTime < 500) return;

  const ttl = parseInt(document.getElementById('ttl-select').value) || 0;

  if (activeGroup) {
    const formData = new FormData();
    formData.append('text', text);
    formData.append('ttl', ttl);
    pendingFiles.forEach(f => formData.append('file', f.file));

    const r = await fetch(
      pendingFiles.length > 0
        ? '/api/groups/' + encodeURIComponent(activeGroup) + '/send-file'
        : '/api/groups/' + encodeURIComponent(activeGroup) + '/send',
      {
        method: 'POST',
        body: pendingFiles.length > 0 ? formData : JSON.stringify({ text, ttl }),
        headers: pendingFiles.length > 0 ? {} : { 'Content-Type': 'application/json' }
      }
    );
    if (!r.ok) {
      const err = await r.text();
      alert('Send failed: ' + err);
      return;
    }
    input.value = '';
    clearPendingFiles();
    const msgs = await fetchMessages(activeGroup);
    renderMessages(msgs, activeGroup);
    return;
  }

  if (!activePeer) return;

  const formData = new FormData();
  formData.append('to', activePeer);
  formData.append('text', text);
  formData.append('ttl', ttl);
  pendingFiles.forEach(f => formData.append('file', f.file));

  const r = await fetch(
    pendingFiles.length > 0
      ? '/api/send-file'
      : '/api/send',
    {
      method: 'POST',
      body: pendingFiles.length > 0 ? formData : JSON.stringify({ to: activePeer, text, ttl }),
      headers: pendingFiles.length > 0 ? {} : { 'Content-Type': 'application/json' }
    }
  );
  if (!r.ok) {
    const err = await r.text();
    alert('Send failed: ' + err);
    return;
  }

  input.value = '';
  clearPendingFiles();
  const msgs = await fetchMessages(activePeer);
  renderMessages(msgs, activePeer);
}

function showChat() {
  document.getElementById('config-panel').style.display = 'none';
  document.getElementById('create-group-panel').style.display = 'none';
  document.getElementById('main').style.display = 'flex';
}

function showConfig() {
  document.getElementById('main').style.display = 'none';
  document.getElementById('no-chat').style.display = 'none';
  document.getElementById('create-group-panel').style.display = 'none';
  document.getElementById('config-panel').style.display = 'flex';
}

function showCreateGroup() {
  document.getElementById('main').style.display = 'none';
  document.getElementById('no-chat').style.display = 'none';
  document.getElementById('config-panel').style.display = 'none';
  document.getElementById('create-group-panel').style.display = 'flex';
  document.getElementById('create-group-status').textContent = '';
  document.getElementById('new-group-name').value = '';
  document.getElementById('new-group-name').focus();
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
  document.getElementById('cfg-peer-id').value = cfg.peer_id || '';
  document.getElementById('cfg-turn-addr').value = cfg.turn_addr || '';
  document.getElementById('cfg-turn-user').value = cfg.turn_user || '';
  document.getElementById('cfg-turn-pass').value = cfg.turn_pass || '';
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
      chunk_ttl: document.getElementById('cfg-chunk-ttl').value,
      peer_id: document.getElementById('cfg-peer-id').value.trim(),
      turn_addr: document.getElementById('cfg-turn-addr').value.trim(),
      turn_user: document.getElementById('cfg-turn-user').value.trim(),
      turn_pass: document.getElementById('cfg-turn-pass').value.trim()
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
      const stillExists = peers.some(p => p.key === activePeer);
      if (!stillExists) {
        activePeer = null;
        document.getElementById('main').style.display = 'none';
        document.getElementById('no-chat').style.display = 'flex';
      }
    }
  });

  evtSource.addEventListener('message', async function(e) {
    const msg = JSON.parse(e.data);
    if (activeGroup) {
      const msgs = await fetchMessages(activeGroup);
      renderMessages(msgs, activeGroup);
    } else if (activePeer && (msg.from === activePeer.split(':')[0] || msg.from === me.username)) {
      const msgs = await fetchMessages(activePeer);
      renderMessages(msgs, activePeer);
    }
  });

  evtSource.onerror = function() {
    console.error('SSE error, reconnecting...');
  };
}

async function inviteToGroup() {
  if (!activeGroup) return;
  const input = document.getElementById('invite-input');
  const userID = input.value.trim();
  if (!userID) return;
  input.value = '';
  const r = await fetch('/api/groups/' + encodeURIComponent(activeGroup) + '/invite', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ user: userID })
  });
  if (!r.ok) {
    const err = await r.text();
    alert('Invite failed: ' + err);
  }
}

async function createGroup() {
  const input = document.getElementById('new-group-name');
  const name = input.value.trim();
  if (!name) return;
  const status = document.getElementById('create-group-status');
  const btn = document.getElementById('create-group-submit');
  btn.disabled = true;
  status.textContent = '';

  const r = await fetch('/api/groups', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name })
  });
  btn.disabled = false;
  if (!r.ok) {
    const err = await r.text();
    status.textContent = 'Error: ' + err;
    status.className = 'cfg-err';
    return;
  }
  const gc = await r.json();
  await fetchGroups();
  document.getElementById('create-group-panel').style.display = 'none';
  document.getElementById('main').style.display = 'flex';
  selectGroup(gc.uid);
}

document.addEventListener('DOMContentLoaded', async function() {
  await fetchMe();
  await fetchPeers();
  await fetchGroups();
  setupSSE();

  document.getElementById('send-btn').addEventListener('click', sendMessage);
  document.getElementById('msg-input').addEventListener('keydown', function(e) {
    if (e.key === 'Enter') {
      const text = document.getElementById('msg-input').value.trim();
      if (!text && pendingFiles.length > 0) return;
      sendMessage();
    }
  });
  document.getElementById('file-input').addEventListener('change', function(e) {
    for (const f of e.target.files) {
      pendingFiles.push({ file: f, name: f.name, size: f.size });
    }
    lastFileAddTime = Date.now();
    e.target.value = '';
    renderPendingFiles();
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
    if (activePeer || activeGroup) {
      (activePeer ? selectPeer(activePeer) : selectGroup(activeGroup));
    } else {
      showChat();
      document.getElementById('no-chat').style.display = 'flex';
    }
  });
  document.getElementById('cfg-save').addEventListener('click', saveConfig);

  document.getElementById('create-group-btn').addEventListener('click', showCreateGroup);
  document.getElementById('create-group-submit').addEventListener('click', createGroup);
  document.getElementById('create-group-cancel').addEventListener('click', function() {
    if (activePeer || activeGroup) {
      document.getElementById('create-group-panel').style.display = 'none';
      document.getElementById('main').style.display = 'flex';
    } else {
      document.getElementById('create-group-panel').style.display = 'none';
      document.getElementById('no-chat').style.display = 'flex';
    }
  });
  document.getElementById('new-group-name').addEventListener('keydown', function(e) {
    if (e.key === 'Enter') createGroup();
  });

  document.getElementById('invite-btn').addEventListener('click', inviteToGroup);
  document.getElementById('invite-input').addEventListener('keydown', function(e) {
    if (e.key === 'Enter') inviteToGroup();
  });
});
