import {
  getAsync, setAsync, isFresh, needsRefresh,
  episodeTTL,
} from "./smartcache.js";
import { getEpisodes as mkissaEpisodes } from "../providers/mkissa.js";
import { getEpisodes as reanimeEpisodes } from "../providers/reanime.js";
import { getEpisodes as anikotoEpisodes } from "../providers/anikoto.js";
import { getEpisodes as animeggEpisodes } from "../providers/animegg.js";
import { getEpisodes as aninekoEpisodes } from "../providers/anineko.js";
import { getEpisodes as anidbappEpisodes } from "../providers/anidbapp.js";
import { getEpisodes as dhiveEpisodes } from "../providers/2dhive.js";
import { getEpisodes as animenosubEpisodes } from "../providers/animenosub.js";
import { getEpisodes as anizoneEpisodes } from "../providers/anizone.js";
import { getEpisodes as aniwavesEpisodes } from "../providers/aniwaves.js";
import { getEpisodes as anibdEpisodes   } from "../providers/anibd.js";
import { getEpisodes as senshiEpisodes } from "../providers/senshi.js";
import { getEpisodes as kaaEpisodes    } from "../providers/kickassanime.js";
import { getEpisodes as animedunyaEpisodes } from "../providers/animedunya.js";
import { getEpisodes as animeonsenEpisodes } from "../providers/animeonsen.js";
const inflight  = new Map();
const bgRunning = new Set();

// Timeout por provedor: um scraper pendurado (ex: captcha/retry) não pode
// segurar a resposta inteira — Promise.all espera o mais lento.
// Env: PROVIDER_TIMEOUT_MS (padrão 15000).
const PROVIDER_TIMEOUT_MS = (() => {
  const n = Number(process?.env?.PROVIDER_TIMEOUT_MS);
  return Number.isFinite(n) && n > 0 ? n : 15000;
})();

function timeoutError(label, ms) {
  const e = new Error(`[ep:${label}] timeout after ${ms}ms`);
  e.code = "PROVIDER_TIMEOUT";
  return e;
}

function withTimeout(promise, label, ms = PROVIDER_TIMEOUT_MS) {
  let t;
  const guard = new Promise((_, reject) => {
    t = setTimeout(() => reject(timeoutError(label, ms)), ms);
  });
  return Promise.finally ? Promise.race([promise, guard]).finally(() => clearTimeout(t))
    : Promise.race([promise, guard]).then(
        (v) => { clearTimeout(t); return v; },
        (e) => { clearTimeout(t); throw e; },
      );
}

// Cache em memória SEMPRE ligado (independe de CACHE_ENABLED, que controla
// disco/Redis). Sem isso, cada /episodes re-scrapeia tudo ao vivo.
// TTL curto: 5 min — suficiente para navegar entre episódios sem re-scrapear.
const MEM_TTL_MS = 5 * 60 * 1000;
const MEM_MAX = 300;
const memFast = new Map();

function memFastGet(key) {
  const e = memFast.get(key);
  if (!e) return null;
  if (Date.now() > e.expiresAt) {
    memFast.delete(key);
    return null;
  }
  return e.data;
}

function memFastSet(key, data, ttlMs = MEM_TTL_MS) {
  memFast.delete(key);
  memFast.set(key, { data, expiresAt: Date.now() + ttlMs });
  if (memFast.size > MEM_MAX) {
    const first = memFast.keys().next().value;
    memFast.delete(first);
  }
}

function dedupe(key, fn) {
  if (inflight.has(key)) return inflight.get(key);
  const p = Promise.resolve().then(fn).finally(() => inflight.delete(key));
  inflight.set(key, p);
  return p;
}

function bg(key, fn) {
  if (bgRunning.has(key)) return;
  bgRunning.add(key);
  Promise.resolve()
    .then(fn)
    .catch(e => console.error(`[bg:${key}]`, e.message))
    .finally(() => bgRunning.delete(key));
}

async function withCache(key, status, fetchFn) {
  // 1) Cache rápido sempre-ligado (memória, 5 min).
  const fast = memFastGet(key);
  if (fast) return fast;

  // 2) Dedupe: requisições concorrentes da mesma chave dividem um scrape só
  // (antes, 5 abas = 5 scrapes simultâneos do mesmo anime).
  return dedupe(key, async () => {
    const recheck = memFastGet(key);
    if (recheck) return recheck;
    const [ttl, refreshAfter] = episodeTTL(status);
    const entry = await getAsync(key);

    if (isFresh(entry)) {
      memFastSet(key, entry.data);
      if (needsRefresh(entry)) {
        bg(key, async () => {
          const data = await withTimeout(fetchFn(), key);
          memFastSet(key, data);
          await setAsync(key, data, ttl, refreshAfter);
        });
      }
      return entry.data;
    }

    const data = await withTimeout(fetchFn(), key);
    memFastSet(key, data);
    await setAsync(key, data, ttl, refreshAfter);
    return data;
  });
}

function orderEpisodeFields(data) {
  if (!data?.episodes || typeof data.episodes !== "object") return data;
  const episodes = Object.fromEntries(Object.entries(data.episodes).map(([audio, list]) => [
    audio,
    Array.isArray(list)
      ? list.map(({ id, audio: itemAudio, sourceNumber, ...episode }) => ({
        ...(id === undefined ? {} : { id }),
        ...(sourceNumber === undefined ? {} : { sourceNumber }),
        ...(itemAudio === undefined ? {} : { audio: itemAudio }),
        ...episode,
      }))
      : list,
  ]));
  return { ...data, episodes };
}

async function safe(label, fn) {
  try   { return { ok: true,  data: orderEpisodeFields(await withTimeout(fn(), label)) }; }
  catch (e) { console.error(`[ep:${label}]`, e.message); return { ok: false, error: e.message, stack: e.stack }; }
}

const PROVIDER_ALIASES = {
  mkissa: "mkissa",
  reanime:  "reanime",
  anikoto:  "anikoto",
  animegg:  "animegg",
  anineko:  "anineko",
  anidbapp: "anidbapp",
  "2dhive": "2dhive",
  animenosub: "animenosub",
  anizone: "anizone",
  aniwaves: "aniwaves",
  anibd:  "anibd",
  senshi: "senshi",
  kaa:    "kaa",
  animedunya: "animedunya",
  animeonsen: "animeonsen",
};

export function resolveProviders(rawNames) {
  const resolved = new Set();
  const unknown  = [];
  for (const raw of rawNames) {
    const name = PROVIDER_ALIASES[raw.toLowerCase()];
    if (name) resolved.add(name);
    else unknown.push(raw);
  }
  return { resolved, unknown };
}

function providerFns(anilistId, status, ctx) {
  return {
    mkissa: () => withCache(`epv:mkissa:${anilistId}`, status, () => mkissaEpisodes(anilistId, ctx)),
    reanime:  () => withCache(`epv:reanime:${anilistId}`, status, () => reanimeEpisodes(anilistId, ctx)),
    anikoto:  () => withCache(`epv:anikoto:${anilistId}`, status, () => anikotoEpisodes(anilistId, ctx)),
    animegg:  () => withCache(`epv:animegg:${anilistId}`, status, () => animeggEpisodes(anilistId, ctx)),
    anineko:  () => withCache(`epv:anineko:${anilistId}`, status, () => aninekoEpisodes(anilistId, ctx)),
    anidbapp: () => withCache(`epv:anidbapp:${anilistId}`, status, () => anidbappEpisodes(anilistId, ctx)),
    "2dhive": () => withCache(`epv:2dhive:${anilistId}`,  status, () => dhiveEpisodes(anilistId, ctx)),
    animenosub: () => withCache(`epv:animenosub:${anilistId}`, status, () => animenosubEpisodes(anilistId, ctx)),
    anizone: () => withCache(`epv:anizone:${anilistId}`, status, () => anizoneEpisodes(anilistId, ctx)),
    aniwaves: () => withCache(`epv:aniwaves:${anilistId}`, status, () => aniwavesEpisodes(anilistId, ctx)),
    anibd:  () => withCache(`epv:anibd:${anilistId}`,   status, () => anibdEpisodes(anilistId, ctx)),
    senshi: () => withCache(`epv:senshi:${anilistId}`,  status, () => senshiEpisodes(anilistId, ctx)),
    kaa:    () => withCache(`epv:kaa:${anilistId}`,     status, () => kaaEpisodes(anilistId, ctx)),
    animedunya: () => withCache(`epv:animedunya:${anilistId}`, status, () => animedunyaEpisodes(anilistId, ctx)),
    animeonsen: () => withCache(`epv:animeonsen:${anilistId}`, status, () => animeonsenEpisodes(anilistId, ctx)),
  };
}

export async function buildFilteredEpisodesWithCache(anilistId, providers, media, anizip) {
  const names = [...providers].sort();
  const aggKey = `epf:${names.join(",")}:${anilistId}`;
  const hit = memFastGet(aggKey);
  if (hit) return hit;

  // Dedupe agregado: mesma combinação em voo compartilha o resultado.
  return dedupe(aggKey, async () => {
    const recheck = memFastGet(aggKey);
    if (recheck) return recheck;
    const status = media?.status ?? "RELEASING";
    const ctx  = { media, anizip, maxPages: undefined };
    const fns  = providerFns(anilistId, status, ctx);

    const pairs = await Promise.all(
      names.map(async (name) => {
        const result = await safe(name, fns[name]);
        return [name, result.ok ? result.data : { error: result.error, stack: result.stack }];
      })
    );

    const out = Object.fromEntries(pairs);
    memFastSet(aggKey, out);
    return out;
  });
}

export async function buildEpisodesWithCache(anilistId, media, anizip) {
  const status = media?.status ?? "RELEASING";
  const ctx = { media, anizip, maxPages: undefined };

  const [mkissa, reanime, anikoto, animegg, anineko, anidbapp, dhive, animenosub, anizone, aniwaves, anibd, senshi, kaa, animedunya, animeonsen] = await Promise.all([
    safe("mkissa",     () => withCache(`epv:mkissa:${anilistId}`,     status, () => mkissaEpisodes(anilistId, ctx))),
    safe("reanime",    () => withCache(`epv:reanime:${anilistId}`,    status, () => reanimeEpisodes(anilistId, ctx))),
    safe("anikoto",    () => withCache(`epv:anikoto:${anilistId}`,    status, () => anikotoEpisodes(anilistId, ctx))),
    safe("animegg",    () => withCache(`epv:animegg:${anilistId}`,    status, () => animeggEpisodes(anilistId, ctx))),
    safe("anineko",    () => withCache(`epv:anineko:${anilistId}`,    status, () => aninekoEpisodes(anilistId, ctx))),
    safe("anidbapp",   () => withCache(`epv:anidbapp:${anilistId}`,   status, () => anidbappEpisodes(anilistId, ctx))),
    safe("2dhive",     () => withCache(`epv:2dhive:${anilistId}`,     status, () => dhiveEpisodes(anilistId, ctx))),
    safe("animenosub", () => withCache(`epv:animenosub:${anilistId}`, status, () => animenosubEpisodes(anilistId, ctx))),
    safe("anizone",    () => withCache(`epv:anizone:${anilistId}`,    status, () => anizoneEpisodes(anilistId, ctx))),
    safe("aniwaves",   () => withCache(`epv:aniwaves:${anilistId}`,   status, () => aniwavesEpisodes(anilistId, ctx))),
    safe("anibd",      () => withCache(`epv:anibd:${anilistId}`,      status, () => anibdEpisodes(anilistId, ctx))),
    safe("senshi",     () => withCache(`epv:senshi:${anilistId}`,     status, () => senshiEpisodes(anilistId, ctx))),
    safe("kaa",        () => withCache(`epv:kaa:${anilistId}`,        status, () => kaaEpisodes(anilistId, ctx))),
    safe("animedunya", () => withCache(`epv:animedunya:${anilistId}`, status, () => animedunyaEpisodes(anilistId, ctx))),
    safe("animeonsen", () => withCache(`epv:animeonsen:${anilistId}`, status, () => animeonsenEpisodes(anilistId, ctx))),
  ]);

  return {
    mkissa:      mkissa.ok      ? mkissa.data      : { error: mkissa.error,      stack: mkissa.stack },
    reanime:     reanime.ok     ? reanime.data     : { error: reanime.error,     stack: reanime.stack },
    anikoto:     anikoto.ok     ? anikoto.data     : { error: anikoto.error,     stack: anikoto.stack },
    animegg:     animegg.ok     ? animegg.data     : { error: animegg.error,     stack: animegg.stack },
    anineko:     anineko.ok     ? anineko.data     : { error: anineko.error,     stack: anineko.stack },
    anidbapp:    anidbapp.ok    ? anidbapp.data    : { error: anidbapp.error,    stack: anidbapp.stack },
    "2dhive":    dhive.ok       ? dhive.data       : { error: dhive.error,       stack: dhive.stack },
    animenosub:  animenosub.ok  ? animenosub.data  : { error: animenosub.error,  stack: animenosub.stack },
    anizone:     anizone.ok     ? anizone.data     : { error: anizone.error,     stack: anizone.stack },
    aniwaves:    aniwaves.ok    ? aniwaves.data    : { error: aniwaves.error,    stack: aniwaves.stack },
    anibd:       anibd.ok       ? anibd.data       : { error: anibd.error,       stack: anibd.stack },
    senshi:      senshi.ok      ? senshi.data      : { error: senshi.error,      stack: senshi.stack },
    kaa:         kaa.ok         ? kaa.data         : { error: kaa.error,         stack: kaa.stack },
    animedunya:  animedunya.ok  ? animedunya.data  : { error: animedunya.error,  stack: animedunya.stack },
    animeonsen:  animeonsen.ok  ? animeonsen.data  : { error: animeonsen.error,  stack: animeonsen.stack },
  };
}
