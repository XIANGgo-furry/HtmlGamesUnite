/* ============================================================
 * HtmlGamesUnite 公共联机接口  HGURelay v1.0
 * 零依赖 · 不引 CDN · 同源页面均可 <script src="relay.js"></script>
 * 首页 index.html 与各游戏 HTML 共用这一份实现，改中继只改本文件。
 * 用法见《全站规范.md》第 9 节。
 * ============================================================ */
(function (root, factory) {
  var api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api; // 便于本地测试
  root.HGURelay = api;
})(typeof self !== 'undefined' ? self : (typeof globalThis !== 'undefined' ? globalThis : this), function () {
  'use strict';

  /* ---------- 极简 MQTT 3.1.1 协议层（与旧版首页逐字节一致） ---------- */
  var MQ = (function () {
    function utf8(s) {
      if (typeof TextEncoder !== 'undefined') return Array.from(new TextEncoder().encode(s));
      var b = unescape(encodeURIComponent(s)), o = [];
      for (var i = 0; i < b.length; i++) o.push(b.charCodeAt(i));
      return o;
    }
    function dec(u) {
      if (typeof TextDecoder !== 'undefined') return new TextDecoder().decode(u);
      var b = ''; for (var i = 0; i < u.length; i++) b += String.fromCharCode(u[i]);
      return decodeURIComponent(escape(b));
    }
    function encLen(n) { var o = []; do { var d = n % 128; n = Math.floor(n / 128); if (n > 0) d |= 0x80; o.push(d); } while (n > 0); return o; }
    function str(s) { var u = utf8(s); return [u.length >> 8, u.length & 255].concat(u); }
    function pack(head, body) { return new Uint8Array([head].concat(encLen(body.length), body)); }
    function connect(cid) { return pack(0x10, [0, 4, 77, 81, 84, 84, 4, 2, 0, 60].concat(str(cid))); }
    function subscribe(topic, id) { return pack(0x82, [id >> 8, id & 255].concat(str(topic), [0])); }
    function publish(topic, data) { return pack(0x30, str(topic).concat(utf8(data))); }
    var ping = function () { return new Uint8Array([0xC0, 0x00]); };
    function readLen(u, at) { var mul = 1, val = 0, i = at, d; do { d = u[i++]; val += (d & 127) * mul; mul *= 128; } while ((d & 128) && i < at + 4); return [val, i]; }
    function parse(buf) {
      var u = new Uint8Array(buf), out = []; var i = 0;
      while (i + 1 < u.length) {
        var type = u[i] >> 4, lr = readLen(u, i + 1), at = lr[1], end = at + lr[0];
        if (end > u.length) break;
        var b = u.slice(at, end);
        if (type === 2) out.push({ type: 'connack', code: b[1] });
        else if (type === 9) out.push({ type: 'suback' });
        else if (type === 13) out.push({ type: 'pingresp' });
        else if (type === 3 && b.length >= 2) {
          var tl = (b[0] << 8) | b[1];
          out.push({ type: 'publish', topic: dec(b.slice(2, 2 + tl)), data: dec(b.slice(2 + tl)) });
        }
        i = end;
      }
      return out;
    }
    return { connect: connect, subscribe: subscribe, publish: publish, ping: ping, parse: parse };
  })();

  /* ---------- 公共中继：国内节点优先，一个不通自动换下一个 ---------- */
  var RELAYS = [
    'wss://broker-cn.emqx.io:8084/mqtt',
    'wss://broker.emqx.io:8084/mqtt',
    'wss://broker.hivemq.com:8884/mqtt',
    'wss://test.mosquitto.org:8081/mqtt',
    'wss://mqtt.eclipseprojects.io:443/mqtt'
  ];

  var TOPIC_PREFIX = 'htmlgamesunite/room/';

  /* ---------- 房间号工具 ---------- */
  function normalizeRoom(r) { return String(r == null ? '' : r).replace(/\D/g, '').slice(0, 6); }
  function randomRoom() {
    var n;
    try { var a = new Uint32Array(1); crypto.getRandomValues(a); n = a[0] % 1000000; }
    catch (e) { n = Math.floor(Math.random() * 1000000); }
    return String(n).padStart(6, '0');
  }
  function roomFromURL(search) {
    try {
      var s = search == null ? (typeof location !== 'undefined' ? location.search : '') : search;
      return normalizeRoom(new URLSearchParams(s).get('room'));
    } catch (e) { return ''; }
  }
  function topicOf(r) { return TOPIC_PREFIX + normalizeRoom(r); }
  function inviteURL(room, file) {
    if (typeof location === 'undefined') return '';
    var p = location.pathname;
    if (file) p = p.replace(/[^/]*$/, file);
    return location.origin + p + '?room=' + normalizeRoom(room);
  }
  function cidNew() {
    try { var a = new Uint32Array(1); crypto.getRandomValues(a); return 'yb' + a[0].toString(36); }
    catch (e) { return 'yb' + Math.random().toString(36).slice(2, 10); }
  }
  function host(u) { try { return new URL(u).host; } catch (e) { return u; } }

  /* ============================================================
   * join(room, opts) → Promise<session>
   *   opts.onStatus(code, text)  code: 'connecting' | 'online' | 'bad'
   *   opts.onMessage(msg)        msg: {id, k, t, ...}（已自动过滤自己发的）
   *   opts.onLog(text)           可选，逐条日志
   *   opts.timeout               单个中继超时毫秒，默认 6000
   *
   * session = {
   *   room, id, topic, broker,
   *   send(k, t)      发一条约定消息，例：send('move','e2e4')
   *   raw(obj)        发任意 JSON，字段原样透传
   *   close()         主动断开
   *   state()         'connecting' | 'online' | 'closed'
   * }
   * 连上后会自动广播 {k:'hello'}，首页大厅因此显示「对方已进房间 ✓」。
   * 消息格式与首页大厅完全兼容，游戏与大厅可互相收发。
   * ============================================================ */
  function join(room, opts) {
    opts = opts || {};
    var r = normalizeRoom(room);
    if (r.length !== 6) return Promise.reject(new Error('房间号必须是 6 位数字'));
    var onStatus = opts.onStatus || function () {};
    var onMessage = opts.onMessage || function () {};
    var onLog = opts.onLog || function () {};
    var timeout = opts.timeout || 6000;

    var id = cidNew(), topic = topicOf(r), idx = 0;
    var ws = null, state = 'connecting', pingT = null, closed = false, settled = false;

    function stopPing() { if (pingT) { clearInterval(pingT); pingT = null; } }
    function startPing() { stopPing(); pingT = setInterval(function () { try { ws.send(MQ.ping()); } catch (e) {} }, 30000); }

    var session = {
      room: r, id: id, topic: topic, broker: '',
      send: function (k, t) { return session.raw({ k: k, t: t }); },
      raw: function (obj) {
        if (state !== 'online' || !ws) return false;
        try {
          var m = Object.assign({}, obj || {}, { id: id });
          ws.send(MQ.publish(topic, JSON.stringify(m)));
          return true;
        } catch (e) { return false; }
      },
      close: function () {
        closed = true; state = 'closed'; stopPing();
        try { if (ws) ws.close(); } catch (e) {}
        onStatus('closed', '已断开');
      },
      state: function () { return state; }
    };

    return new Promise(function (resolve, reject) {
      function failAll() {
        state = 'closed'; stopPing();
        onStatus('bad', '中继暂时连不上 · 单机照常能玩');
        onLog('5 个公共中继都没连上，可能是网络限制，稍后再试。');
        reject(new Error('所有公共中继均不可用'));
      }
      function tryNext() {
        if (closed) return;
        if (idx >= RELAYS.length) { failAll(); return; }
        var url = RELAYS[idx++], sock, done = false, timer = null;
        var giveUp = function () {
          if (done || settled) return; done = true;
          clearTimeout(timer);
          try { sock.close(); } catch (e) {}
          tryNext();
        };
        try { sock = new WebSocket(url, 'mqtt'); }
        catch (e) { tryNext(); return; }
        sock.binaryType = 'arraybuffer';
        timer = setTimeout(giveUp, timeout);
        onStatus('connecting', '正在握手 · ' + host(url));
        sock.onopen = function () {
          try { sock.send(MQ.connect(id)); } catch (e) { giveUp(); }
        };
        sock.onmessage = function (ev) {
          var pkts; try { pkts = MQ.parse(ev.data); } catch (e) { return; }
          for (var i = 0; i < pkts.length; i++) {
            var p = pkts[i];
            if (p.type === 'connack') {
              if (p.code !== 0) { giveUp(); return; }
              settled = true; done = true; clearTimeout(timer);
              ws = sock; state = 'online'; session.broker = host(url);
              onStatus('online', '已连接 · ' + host(url));
              try {
                sock.send(MQ.subscribe(topic, 1));
                sock.send(MQ.publish(topic, JSON.stringify({ id: id, k: 'hello' })));
              } catch (e) {}
              startPing();
              onLog('连上了 ' + host(url) + '，房间 ' + r + ' 已就绪');
              resolve(session);
              return;
            }
            if (p.type === 'publish') {
              var m; try { m = JSON.parse(p.data); } catch (e) { continue; }
              if (!m || m.id === id) continue;
              onMessage(m, p.topic);
            }
          }
        };
        sock.onerror = giveUp;
        sock.onclose = function () {
          if (!settled) { giveUp(); return; }
          if (!closed && state === 'online') {
            state = 'closed'; stopPing();
            onStatus('bad', '连接已断开 · 可重新 join');
            onLog('连接断开了，重新 join 即可。');
          }
        };
      }
      onStatus('connecting', '连接中…逐个试公共中继');
      tryNext();
    });
  }

  return {
    version: '1.0',
    MQ: MQ,
    RELAYS: RELAYS,
    TOPIC_PREFIX: TOPIC_PREFIX,
    join: join,
    topicOf: topicOf,
    normalizeRoom: normalizeRoom,
    randomRoom: randomRoom,
    roomFromURL: roomFromURL,
    inviteURL: inviteURL
  };
});
