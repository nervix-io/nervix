import asyncio
import ssl

from aiohttp import WSMsgType, web


served_http = set()
active_websockets = {}


async def handle_http(request: web.Request) -> web.StreamResponse:
    path = request.path
    print(f"http request path={path}", flush=True)
    if path in served_http:
        return web.Response(status=204)

    served_http.add(path)
    await asyncio.sleep(2)
    return web.Response(
        status=200,
        body=b'{"user_id":42}',
        content_type="application/json",
    )


async def handle_ws(request: web.Request) -> web.StreamResponse:
    path = request.path
    print(f"websocket connect path={path}", flush=True)
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    active_websockets.setdefault(path, set()).add(ws)

    try:
        await asyncio.sleep(2)
        print(f"websocket send path={path}", flush=True)
        await ws.send_str('{"user_id":42}')

        async for msg in ws:
            if msg.type == WSMsgType.CLOSE:
                break
    finally:
        connections = active_websockets.get(path)
        if connections is not None:
            connections.discard(ws)
            if not connections:
                active_websockets.pop(path, None)

    return ws


async def publish_ws(request: web.Request) -> web.StreamResponse:
    path = request.path
    connections = [ws for ws in active_websockets.get(path, ()) if not ws.closed]
    if not connections:
        return web.Response(status=409, text="no active websocket connection")

    payload = await request.text()
    sent = 0
    for ws in connections:
        try:
            await ws.send_str(payload)
            sent += 1
        except Exception as error:
            print(f"websocket publish failed path={path} error={error}", flush=True)
            active_websockets[path].discard(ws)
    if not active_websockets.get(path):
        active_websockets.pop(path, None)
    if sent == 0:
        return web.Response(status=409, text="no active websocket connection")
    print(
        f"websocket publish path={path} connections={sent}",
        flush=True,
    )
    return web.Response(status=200, text=str(sent))


app = web.Application()
app.router.add_get("/http/{name}", handle_http)
app.router.add_get("/ws/{name}", handle_ws)
app.router.add_post("/ws/{name}", publish_ws)


async def start() -> None:
    runner = web.AppRunner(app)
    await runner.setup()

    http_site = web.TCPSite(runner, "0.0.0.0", 8080)
    tls_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    tls_context.load_cert_chain("/certs/node.pem", "/certs/node-key.pem")
    https_site = web.TCPSite(runner, "0.0.0.0", 8443, ssl_context=tls_context)

    await http_site.start()
    await https_site.start()
    print("mock server ready", flush=True)

    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(start())
