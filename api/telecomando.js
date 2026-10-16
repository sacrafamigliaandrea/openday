/*
  Relay del telecomando iPhone → presentazione (Open Day · Campus Sacra Famiglia).

  Funzione Vercel senza dipendenze: parla con Upstash Redis tramite la sua API REST (fetch).
  Variabili d'ambiente create dall'integrazione Upstash del pannello Vercel:
    KV_REST_API_URL + KV_REST_API_TOKEN   (oppure UPSTASH_REDIS_REST_URL + UPSTASH_REDIS_REST_TOKEN)

  Protocollo (tutte richieste "semplici": GET, oppure POST con testo JSON → nessun preflight CORS):
    GET  ?ping                         → {ok, redis}            verifica del collegamento
    GET  ?ruolo=show&c=CANALE[&dopo=N] → {seq, cmds:[{seq,cmd}]}  la presentazione chiede i comandi nuovi
    GET  ?ruolo=remote&c=CANALE        → {state, now}            il telecomando legge lo stato
    POST {c, cmd:"next"|"prev"|"black"} → {seq}                  il telecomando invia un comando
    POST {c, state:{cur,step,black}}    → {ok}                   la presentazione pubblica lo stato

  Il CANALE è il codice segreto scelto da chi presenta: senza, nessuno può comandare le slide.
  Tutto scade da solo dopo 6 ore.
*/
const REDIS_URL = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const REDIS_TOKEN = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;
const TTL = 6 * 3600;          // secondi
const MAX_AGE = 8000;          // ms: un comando più vecchio non viene più eseguito (es. dopo un buco di rete)
const COMANDI = ['next', 'prev', 'black'];

async function redis(cmds) {
  const r = await fetch(REDIS_URL + '/pipeline', {
    method: 'POST',
    headers: {Authorization: 'Bearer ' + REDIS_TOKEN, 'Content-Type': 'application/json'},
    body: JSON.stringify(cmds)
  });
  if (!r.ok) throw new Error('redis ' + r.status);
  return (await r.json()).map(x => x.result);
}

function bodyOf(req) {
  if (req.body && typeof req.body === 'object') return req.body;
  try { return JSON.parse(req.body || '{}'); } catch (e) { return {}; }
}

module.exports = async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  res.setHeader('Access-Control-Allow-Origin', '*');      // la presentazione può girare anche da file locale
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(204).end();
  if (!REDIS_URL || !REDIS_TOKEN) return res.status(500).json({error: 'Upstash Redis non collegato al progetto'});

  const q = req.method === 'GET' ? (req.query || {}) : bodyOf(req);
  try {
    if (req.method === 'GET' && 'ping' in q) {
      await redis([['PING']]);
      return res.json({ok: true, redis: true});
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
    return res.status(502).json({error: 'Redis non raggiungibile'});
  }
};
