const ALIAS_PREFIXES = new Set(["snapshot", "latest", "stable", "develop"]);

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const pathname = url.pathname.replace(/^\/+/, "");
    const redirectTarget = await resolveAliasRedirect(pathname, env);
    if (redirectTarget !== null) {
      url.pathname = `/${redirectTarget}`;
      return Response.redirect(url.toString(), 307);
    }

    let key = normalizeStorageKey(pathname);

    let object = await env.DOCS_BUCKET.get(key);
    if (object === null && !key.endsWith(".html")) {
      object = await env.DOCS_BUCKET.get(`${key}.html`);
      if (object !== null) {
        key = `${key}.html`;
      }
    }

    if (object === null) {
      return new Response("Not Found", {
        status: 404,
        headers: {
          "content-type": "text/plain; charset=utf-8",
        },
      });
    }

    const headers = new Headers();
    object.writeHttpMetadata(headers);
    headers.set("etag", object.httpEtag);
    headers.set("cache-control", cacheControlFor(pathname, key));

    return new Response(object.body, {
      headers,
    });
  },
};

function normalizeStorageKey(pathname) {
  if (pathname === "") {
    return "index.html";
  }
  if (pathname.endsWith("/")) {
    return `${pathname}index.html`;
  }
  return pathname;
}

async function resolveAliasRedirect(pathname, env) {
  if (pathname === "") {
    return "snapshot/";
  }

  const segments = pathname.split("/");
  const alias = segments[0];
  if (!ALIAS_PREFIXES.has(alias)) {
    return null;
  }

  const aliasObject = await env.DOCS_BUCKET.get(`meta/${alias}.txt`);
  if (aliasObject === null) {
    return null;
  }

  const targetPrefix = (await aliasObject.text())
    .trim()
    .replace(/^\/+|\/+$/g, "");
  if (targetPrefix === "") {
    return null;
  }

  const rest = segments.slice(1).join("/");
  if (rest === "") {
    return `${targetPrefix}/`;
  }
  return `${targetPrefix}/${rest}`;
}

function cacheControlFor(requestPath, resolvedKey) {
  const alias = requestPath.split("/")[0];
  if (ALIAS_PREFIXES.has(alias)) {
    return "public, max-age=300";
  }
  if (resolvedKey.startsWith("meta/")) {
    return "public, max-age=60";
  }
  return "public, max-age=31536000, immutable";
}
