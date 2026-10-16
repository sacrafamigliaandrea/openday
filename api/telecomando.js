/*
  Relay del telecomando iPhone → presentazione (Open Day · Campus Sacra Famiglia).

  Funzione Vercel senza dipendenze. Trova da sola il database nelle variabili d'ambiente:
    1. API REST di Upstash: …REST_API_URL + …REST_API_TOKEN, oppure …REDIS_REST_URL + …REDIS_REST_TOKEN,
       con qualunque prefisso scelto collegando il database (KV_, STORAGE_, UPSTASH_…)
    2. altrimenti un indirizzo redis:// o rediss:// (REDIS_URL, KV_URL, …): protocollo Redis
       parlato direttamente con i moduli net/tls di Node.
  GET ?ping dice quale via usa, oppure elenca i NOMI delle variabili trovate (mai i valori).

  Protocollo (tutte richieste "semplici": GET, oppure POST con testo JSON → nessun preflight CORS):
    GET  ?ping                         → {ok, redis}            verifica del collegamento
    GET  ?ruolo=show&c=CANALE[&dopo=N] → {seq, cmds:[{seq,cmd}]}  la presentazione chiede i comandi nuovi
    GET  ?ruolo=remote&c=CANALE        → {state, now}            il telecomando legge lo stato
    POST {c, cmd:"next"|"prev"|"black"} → {seq}                  il telecomando invia un comando
    POST {c, state:{cur,step,black}}    → {ok}                   la presentazione pubblica lo stato

  Il CANALE è il codice segreto scelto da chi presenta: senza, nessuno può comandare le slide.
  Tutto scade da solo dopo 6 ore.
*/
const net = require('net'), tls = require('tls');
const TTL = 6 * 3600;          // secondi
const MAX_AGE = 8000;          // ms: un comando più vecchio non viene più eseguito (es. dopo un buco di rete)
const COMANDI = ['next', 'prev', 'black'];

/* ── dove sta il database ── */
function findDb() {
  const e = process.env;
  for (const k of Object.keys(e)) {                    // REST di Upstash, con qualunque prefisso
    let m = k.match(/^(.*)REST_API_URL$/);
    if (m && e[m[1] + 'REST_API_TOKEN']) return {via: 'rest', url: e[k], token: e[m[1] + 'REST_API_TOKEN']};
    m = k.match(/^(.*)REDIS_REST_URL$/);
    if (m && e[m[1] + 'REDIS_REST_TOKEN']) return {via: 'rest', url: e[k], token: e[m[1] + 'REDIS_REST_TOKEN']};
  }
  for (const k of Object.keys(e)) {                    // indirizzo Redis classico
    if (/^rediss?:\/\//.test(e[k] || '')) return {via: 'tcp', url: e[k]};
  }
  return null;
}
const DB = findDb();
const envNames = () => Object.keys(process.env).filter(k => /KV|REDIS|UPSTASH|REST_API/i.test(k)).sort();

/* ── via REST (Upstash) ── */
async function viaRest(cmds) {
  const r = await fetch(DB.url.replace(/\/$/, '') + '/pipeline', {
    method: 'POST',
    headers: {Authorization: 'Bearer ' + DB.token, 'Content-Type': 'application/json'},
    body: JSON.stringify(cmds)
  });
  if (!r.ok) throw new Error('redis REST ' + r.status);
  return (await r.json()).map(x => { if (x.error) throw new Error(x.error); return x.result; });
}

/* ── via protocollo Redis (RESP) su TCP/TLS, senza librerie ── */
function respEncode(args) {
  return '*' + args.length + '\r\n' + args.map(a => { const s = String(a); return '$' + Buffer.byteLength(s) + '\r\n' + s + '\r\n'; }).join('');
}
function respParse(buf, i) {                          // → [valore, prossimo indice] oppure null se incompleto
  if (i >= buf.length) return null;
  const end = buf.indexOf('\r\n', i);
  if (end < 0) return null;
  const t = String.fromCharCode(buf[i]), line = buf.toString('utf8', i + 1, end);
  if (t === '+') return [line, end + 2];
  if (t === '-') return [new Error(line), end + 2];
  if (t === ':') return [parseInt(line, 10), end + 2];
  if (t === '$') {
    const n = parseInt(line, 10);
    if (n < 0) return [null, end + 2];
    if (buf.length < end + 2 + n + 2) return null;
    return [buf.toString('utf8', end + 2, end + 2 + n), end + 4 + n];
  }
  if (t === '*') {
    const n = parseInt(line, 10), out = [];
    let j = end + 2;
    for (let k = 0; k < n; k++) { const r = respParse(buf, j); if (!r) return null; out.push(r[0]); j = r[1]; }
    return [n < 0 ? null : out, j];
  }
  throw new Error('risposta Redis non riconosciuta');
}
function viaTcp(cmds) {
  return new Promise((resolve, reject) => {
    const u = new URL(DB.url);
    const all = [];
    if (u.password) all.push(u.username ? ['AUTH', decodeURIComponent(u.username), decodeURIComponent(u.password)] : ['AUTH', decodeURIComponent(u.password)]);
    const skip = all.length;
    all.push(...cmds);
    const opts = {host: u.hostname, port: +u.port || 6379, servername: u.hostname};
    const sock = u.protocol === 'rediss:' ? tls.connect(opts) : net.connect(opts);
    let buf = Buffer.alloc(0), done = false;
    const finish = (err, val) => { if (done) return; done = true; clearTimeout(timer); sock.destroy(); err ? reject(err) : resolve(val); };
    const timer = setTimeout(() => finish(new Error('redis TCP: tempo scaduto')), 5000);
    sock.on('error', finish);
    sock.on(u.protocol === 'rediss:' ? 'secureConnect' : 'connect', () => sock.write(all.map(respEncode).join('')));
    sock.on('data', d => {
      buf = Buffer.concat([buf, d]);
      const out = []; let i = 0;
      while (out.length < all.length) { const r = respParse(buf, i); if (!r) return; out.push(r[0]); i = r[1]; }
      const err = out.find(x => x instanceof Error);
      finish(err || null, out.slice(skip));
    });
  });
}

const redis = cmds => DB.via === 'rest' ? viaRest(cmds) : viaTcp(cmds);

function bodyOf(req) {
  if (req.body && typeof req.body === 'object') return req.body;
  try { return JSON.parse(req.body || '{}'); } catch (e) { return {}; }
}

module.exports = async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  res.setHeader('Access-Control-Allow-Origin', '*');      // la presentazione può girare anche da file locale
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(204).end();
  const q = req.method === 'GET' ? (req.query || {}) : bodyOf(req);
  if (!DB) return res.status(500).json({
    error: 'Nessun database Redis nelle variabili del progetto: collega Upstash e fai Redeploy',
    variabili_trovate: envNames()
  });
  try {
    if (req.method === 'GET' && 'ping' in q) {
      await redis([['PING']]);
      return res.json({ok: true, redis: true, via: DB.via});
    }
    const c = String(q.c || '');
    if (!/^[A-Za-z0-9_-]{6,64}$/.test(c)) return res.status(400).json({error: 'canale non valido (6-64 lettere, numeri, - o _)'});
    const k = 'od:' + c + ':';

    if (req.method === 'GET' && q.ruolo === 'show') {
      const [seqRaw] = await redis([['GET', k + 'seq']]);
      const seq = parseInt(seqRaw || '0', 10);
      const dopo = parseInt(q.dopo, 10);
      // primo contatto (o niente di nuovo): solo il numero dell'ultimo comando, niente da eseguire
      if (isNaN(dopo) || seq <= dopo) return res.json({seq, cmds: []});
      const [list] = await redis([['LRANGE', k + 'cmds', 0, -1]]);
      const now = Date.now();
      const cmds = (list || []).map(x => JSON.parse(x)).filter(x => x.seq > dopo && now - x.t < MAX_AGE)
        .map(x => ({seq: x.seq, cmd: x.cmd}));
      return res.json({seq, cmds});
    }

    if (req.method === 'GET' && q.ruolo === 'remote') {
      const [state] = await redis([['GET', k + 'state']]);
      return res.json({state: state ? JSON.parse(state) : null, now: Date.now()});
    }

    if (req.method === 'POST' && q.cmd) {
      if (!COMANDI.includes(q.cmd)) return res.status(400).json({error: 'comando non valido'});
      const [seq] = await redis([['INCR', k + 'seq']]);
      await redis([
        ['RPUSH', k + 'cmds', JSON.stringify({seq, cmd: q.cmd, t: Date.now()})],
        ['LTRIM', k + 'cmds', -20, -1],
        ['EXPIRE', k + 'cmds', TTL],
        ['EXPIRE', k + 'seq', TTL]
      ]);
      return res.json({seq});
    }

    if (req.method === 'POST' && q.state && typeof q.state === 'object') {
      const st = {cur: +q.state.cur || 0, step: +q.state.step || 0, black: !!q.state.black, t: Date.now()};
      await redis([['SET', k + 'state', JSON.stringify(st), 'EX', TTL]]);
      return res.json({ok: true});
    }

    return res.status(400).json({error: 'richiesta non valida'});
  } catch (e) {
    return res.status(502).json({error: 'Redis non raggiungibile', via: DB.via, dettaglio: String(e.message || e).slice(0, 120)});
  }
};
