import { getMedia }                from "./core/anilist.js";
import { mapAnimeIds }             from "./core/mapper.js";
import mkissaHandler               from "./providers/mkissa.js";
import reanimeHandler              from "./providers/reanime.js";
import anikotoHandler              from "./providers/anikoto.js";
import animeggHandler              from "./providers/animegg.js";
import aninekoHandler              from "./providers/anineko.js";
import anidbappHandler             from "./providers/anidbapp.js";
import dhiveHandler                from "./providers/2dhive.js";
import animenosubHandler           from "./providers/animenosub.js";
import anizoneHandler              from "./providers/anizone.js";
import aniwavesHandler             from "./providers/aniwaves.js";
import anibdHandler                from "./providers/anibd.js";
import senshiHandler               from "./providers/senshi.js";
import kaaHandler                  from "./providers/kickassanime.js";
import animedunyaHandler           from "./providers/animedunya.js";
import animeonsenHandler           from "./providers/animeonsen.js";
import { getEpisodesResponse, getFilteredEpisodesResponse } from "./core/episode-cache.js";
import { resolveProviders }         from "./core/episode-strategy.js";
import { getAsync, setAsync, isFresh, mapTTL, WATCH_TTL, _CACHE_ENABLED } from "./core/smartcache.js";

function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Cache-Control": "public, max-age=300",
    },
  });
}

function rewriteRequest(request, newPath) {
  const u = new URL(request.url);
  u.pathname = newPath;
  return new Request(u.toString(), { method: request.method, headers: request.headers });
}

function proxyPlaylist(text, base, selfBase, ref) {
  const abs = (uri) => {
    try {
      return new URL(uri, base).toString();
    } catch {
      return null;
    }
  };
  const proxied = (u) => `${selfBase}/p?u=${encodeURIComponent(u)}${ref ? `&ref=${encodeURIComponent(ref)}` : ""}`;
  return String(text)
    .split("\n")
    .map((line) => {
      const t = line.trim();
      if (!t) return line;
      if (t.startsWith("#")) {
        return line.replace(/URI="([^"]+)"/g, (m2, g) => {
          const a = abs(g);
          return a ? `URI="${proxied(a)}"` : m2;
        });
      }
      const a = abs(t);
      return a ? proxied(a) : line;
    })
    .join("\n");
}

async function proxyStream(request) {
  const url = new URL(request.url);
  const target = url.searchParams.get("u") || "";
  const ref = url.searchParams.get("ref") || "";
  let upstreamUrl;
  try {
    upstreamUrl = new URL(target);
  } catch {
    return json({ error: "Parâmetro u inválido." }, 400);
  }
  if (upstreamUrl.protocol !== "http:" && upstreamUrl.protocol !== "https:") {
    return json({ error: "URL não permitida." }, 400);
  }

  const headers = {
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    Accept: "*/*",
  };
  if (ref) {
    headers.Referer = ref;
    try {
      headers.Origin = new URL(ref).origin;
    } catch {}
  }
  const range = request.headers.get("Range");
  if (range) headers.Range = range;

  const ctrl = new AbortController();
  const timeout = setTimeout(() => ctrl.abort(), 30000);
  try {
    const upstream = await fetch(upstreamUrl.toString(), { headers, signal: ctrl.signal });
    if (!upstream.ok && upstream.status !== 206) {
      return json({ error: `Upstream HTTP ${upstream.status}.` }, 502);
    }
    const outHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Expose-Headers": "Content-Range, Content-Length, Accept-Ranges",
      "Cache-Control": "public, max-age=60",
    };
    const contentType = upstream.headers.get("Content-Type") || "";
    // Sempre https em produção (o TLS termina no proxy do host).
    // http:// explícito numa página https:// = mixed content bloqueado.
    const isLocal = /^(localhost|127\.0\.0\.1)/i.test(url.host);
    const selfBase = `${isLocal ? "http" : "https"}://${url.host}`;
    if (/mpegurl|vnd\.apple|x-mpegurl/i.test(contentType) || /\.m3u8($|\?)/i.test(upstreamUrl.pathname + upstreamUrl.search)) {
      const text = await upstream.text();
      outHeaders["Content-Type"] = "application/vnd.apple.mpegurl";
      return new Response(proxyPlaylist(text, upstreamUrl.toString(), selfBase, ref), {
        status: 200,
        headers: outHeaders,
      });
    }
    for (const h of ["Content-Type", "Content-Length", "Content-Range", "Accept-Ranges"]) {
      const v = upstream.headers.get(h);
      if (v) outHeaders[h] = v;
    }
    return new Response(upstream.body, { status: upstream.status, headers: outHeaders });
  } catch (e) {
    return json({ error: "Falha ao buscar stream.", detail: String(e?.message || e) }, 502);
  } finally {
    clearTimeout(timeout);
  }
}

const watchInflight = new Map();
const SIGNED_STREAM_WATCH_TTL = 60_000;

async function cachedWatch(cacheKey, handlerFn, ttl = WATCH_TTL) {
  const entry = await getAsync(cacheKey);
  if (entry && isFresh(entry)) return json(entry.data);

  if (watchInflight.has(cacheKey)) {
    await watchInflight.get(cacheKey).catch(() => {});
    const warm = await getAsync(cacheKey);
    if (warm && isFresh(warm)) return json(warm.data);
    return handlerFn();
  }

  const promise = (async () => {
    const response = await handlerFn();
    if (response.status === 200) {
      try {
        const data = await response.clone().json();
        await setAsync(cacheKey, data, ttl);
      } catch {}
    }
    return response;
  })();

  watchInflight.set(cacheKey, promise);
  try   { return await promise; }
  finally { watchInflight.delete(cacheKey); }
}

export default {
  async fetch(request, env) {
    const url  = new URL(request.url);
    const path = url.pathname;

    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: {
          "Access-Control-Allow-Origin":  "*",
          "Access-Control-Allow-Methods": "GET, OPTIONS",
          "Access-Control-Allow-Headers": "*",
        },
      });
    }

    let m = path.match(/^\/map\/(\d+)\/?$/);
    if (m) {
      const anilistId = m[1];
      const cacheKey  = `map:${anilistId}`;
      const entry     = await getAsync(cacheKey);
      if (entry && isFresh(entry)) return json(entry.data);

      try {
        const [data, media] = await Promise.all([
          mapAnimeIds(anilistId),
          getMedia(anilistId).catch(() => null),
        ]);
        await setAsync(cacheKey, data, mapTTL(media?.status ?? "RELEASING"));
        return json(data);
      } catch (e) {
        if (entry) return json(entry.data);
        return json({ error: e.message }, 500);
      }
    }

    m = path.match(/^\/episodes\/((?:[\w-]+\/)+)(\d+)\/?$/i);
    if (m) {
      const rawNames  = m[1].replace(/\/$/, "").split("/");
      const anilistId = m[2];
      const includeMap = url.searchParams.get("map") !== "false";
      const { resolved, unknown } = resolveProviders(rawNames);

      if (resolved.size === 0) {
        return json({ error: "No valid providers specified", unknown }, 400);
      }

      try {
        const data = await getFilteredEpisodesResponse(anilistId, resolved, includeMap);
        if (unknown.length) data._unknownProviders = unknown;
        return json(data);
      } catch (e) {
        return json({ error: e.message }, 500);
      }
    }

    m = path.match(/^\/episodes\/(\d+)\/?$/);
    if (m) {
      const anilistId = m[1];
      try {
        return json(await getEpisodesResponse(anilistId, env));
      } catch (e) {
        return json({ error: e.message }, 500);
      }
    }

    m = path.match(/^\/watch\/mkissa\/(\d+)\/(sub|dub)\/mkissa-(\d+)\/?$/);
    if (m) {
      const [, id, audio, ep] = m;
      return cachedWatch(
        `watch:mkissa:${id}:${audio}:${ep}`,
        () => mkissaHandler.fetch(request)
      );
    }

    if (path.match(/^\/captcha\/mkissa\/?$/)) {
      return mkissaHandler.fetch(request);
    }

    m = path.match(/^\/watch\/reanime\/(\d+)\/(sub|dub)\/reanime-(\d+)\/?$/);
    if (m) {
      const [, id, audio, ep] = m;
      return cachedWatch(
        `watch:reanime:${id}:${audio}:${ep}`,
        () => reanimeHandler.fetch(rewriteRequest(request, `/watch/${id}/${audio}/${ep}`))
      );
    }

    m = path.match(/^\/stream\/reanime\/(\d+)\/(sub|dub)\/(\d+)\/?$/);
    if (m) {
      const [, id, audio, ep] = m;
      return reanimeHandler.fetch(rewriteRequest(request, `/stream/${id}/${audio}/${ep}`));
    }

    m = path.match(/^\/watch\/anikoto\/(\d+)\/(sub|dub)\/anikoto-(\d+)\/?$/);
    if (m) {
      const [, id, audio, ep] = m;
      return cachedWatch(
        `watch:anikoto:${id}:${audio}:${ep}`,
        () => anikotoHandler.fetch(request),
        SIGNED_STREAM_WATCH_TTL
      );
    }

    m = path.match(/^\/watch\/animegg\/(\d+)\/(sub|dub)\/animegg-(\d+)\/?$/);
    if (m) {
      const [, id, audio, ep] = m;
      return cachedWatch(
        `watch:animegg:${id}:${audio}:${ep}`,
        () => animeggHandler.fetch(request)
      );
    }

    m = path.match(/^\/watch\/anineko\/(\d+)\/(sub|dub)\/anineko-(\d+)\/?$/);
    if (m) {
      const [, id, audio, ep] = m;
      return cachedWatch(
        `watch:anineko:${id}:${audio}:${ep}`,
        () => aninekoHandler.fetch(request)
      );
    }

    m = path.match(/^\/watch\/anidbapp\/(\d+)\/(sub|dub)\/anidbapp-(\d+)\/?$/);
    if (m) {
      const [, id, audio, ep] = m;
      return cachedWatch(
        `watch:anidbapp:${id}:${audio}:${ep}`,
        () => anidbappHandler.fetch(request)
      );
    }

    m = path.match(/^\/watch\/2dhive\/(\d+)\/(sub|dub)\/2dhive-(\d+)\/?$/);
    if (m) {
      const [, id, audio, ep] = m;
      return cachedWatch(
        `watch:2dhive:${id}:${audio}:${ep}`,
        () => dhiveHandler.fetch(request),
        SIGNED_STREAM_WATCH_TTL
      );
    }

    m = path.match(/^\/watch\/animenosub\/(\d+)\/(sub|dub)\/animenosub-(\d+)\/?$/);
    if (m) {
      const [, id, audio, ep] = m;
      return cachedWatch(
        `watch:animenosub:${id}:${audio}:${ep}`,
        () => animenosubHandler.fetch(request)
      );
    }

    m = path.match(/^\/watch\/anizone\/(\d+)\/(sub|dub)\/anizone-(\d+)\/?$/);
    if (m) {
      const [, id, audio, ep] = m;
      return cachedWatch(
        `watch:anizone:${id}:${audio}:${ep}`,
        () => anizoneHandler.fetch(request)
      );
    }

    m = path.match(/^\/watch\/aniwaves\/(\d+)\/(sub|dub)\/aniwaves-(\d+)\/?$/);
    if (m) {
      const [, id, audio, ep] = m;
      return cachedWatch(
        `watch:aniwaves:${id}:${audio}:${ep}`,
        () => aniwavesHandler.fetch(request)
      );
    }

    m = path.match(/^\/watch\/anibd\/(\d+)\/(sub|dub)\/anibd-(\d+)\/?$/);
    if (m) {
      const [, id, audio, ep] = m;
      return cachedWatch(
        `watch:anibd:${id}:${audio}:${ep}`,
        () => anibdHandler.fetch(request)
      );
    }

    m = path.match(/^\/watch\/senshi\/(\d+)\/(sub|dub)\/senshi-(\d+)\/?$/);
    if (m) {
      const [, id, audio, ep] = m;
      return cachedWatch(
        `watch:senshi:${id}:${audio}:${ep}`,
        () => senshiHandler.fetch(request),
        SIGNED_STREAM_WATCH_TTL
      );
    }

    m = path.match(/^\/watch\/kaa\/(\d+)\/(sub|dub)\/kaa-(\d+)\/?$/);
    if (m) {
      const [, id, audio, ep] = m;
      return cachedWatch(
        `watch:kaa:${id}:${audio}:${ep}`,
        () => kaaHandler.fetch(request)
      );
    }

    m = path.match(/^\/watch\/animedunya\/(\d+)\/(sub|dub)\/animedunya-(\d+)\/?$/);
    if (m) {
      const [, id, audio, ep] = m;
      return cachedWatch(
        `watch:animedunya:${id}:${audio}:${ep}`,
        () => animedunyaHandler.fetch(request)
      );
    }

    m = path.match(/^\/watch\/animeonsen\/(\d+)\/(sub|dub)\/animeonsen-(\d+)\/?$/);
    if (m) {
      const [, id, audio, ep] = m;
      return cachedWatch(
        `watch:animeonsen:${id}:${audio}:${ep}`,
        () => animeonsenHandler.fetch(request)
      );
    }

    m = path.match(/^\/stream\/2dhive\/(\d+)\/(sub|dub)\/(\d+)\/?$/);
    if (m) return dhiveHandler.fetch(request);

    m = path.match(/^\/stream\/2dhive\/download\/(\d+)\/(sub|dub)\/(\d+)\/?$/);
    if (m) return dhiveHandler.fetch(request);

    // Proxy genérico de stream (HLS/MP4/legendas) para players web.
    // CDNs bloqueiam CORS por origin e/ou IPs de serverless — como este
    // servidor já acessa essas CDNs no scrape, ele repassa com Referer e CORS *.
    // Uso: GET /p?u=<url>&ref=<referer-opcional>
    // Playlists .m3u8 são reescritas para continuarem passando por aqui.
    if (path.match(/^\/p\/?$/)) {
      return proxyStream(request);
    }

    return json({
      name: "Anivexa API 2.2.1",
      cache: _CACHE_ENABLED,
      providers: [
        "mkissa",
        "reanime",
        "anikoto",
        "animegg",
        "anineko",
        "anidbapp",
        "2dhive",
        "animenosub",
        "anizone",
        "aniwaves",
        "anibd",
        "senshi",
        "kaa",
        "animedunya",
        "animeonsen",
      ],
      routes: [
        "/map/:anilistId",
        "/episodes/:anilistId",
        "/episodes/:provider[/:provider...]/:anilistId?map=true|false",
        "/p?u=<url>&ref=<referer> (proxy de stream HLS/MP4 com CORS)",
        "/watch/mkissa/:id/sub|dub/mkissa-:ep",
        "/watch/reanime/:id/sub|dub/reanime-:ep",
        "/stream/reanime/:id/sub|dub/:ep",
        "/watch/anikoto/:id/sub|dub/anikoto-:ep",
        "/watch/animegg/:id/sub|dub/animegg-:ep",
        "/watch/anineko/:id/sub|dub/anineko-:ep",
        "/watch/anidbapp/:id/sub|dub/anidbapp-:ep",
        "/watch/2dhive/:id/sub|dub/2dhive-:ep",
        "/stream/2dhive/:id/sub|dub/:ep",
        "/stream/2dhive/download/:id/sub|dub/:ep",
        "/watch/animenosub/:id/sub|dub/animenosub-:ep",
        "/watch/anizone/:id/sub|dub/anizone-:ep",
        "/watch/aniwaves/:id/sub|dub/aniwaves-:ep",
        "/watch/anibd/:id/sub|dub/anibd-:ep",
        "/watch/senshi/:id/sub|dub/senshi-:ep",
        "/watch/kaa/:id/sub|dub/kaa-:ep",
        "/watch/animedunya/:id/sub|dub/animedunya-:ep",
        "/watch/animeonsen/:id/sub|dub/animeonsen-:ep",
      ],
    });
  },
};
