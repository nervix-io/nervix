import asyncio
import ssl

from aiohttp import WSMsgType, web


served_http = set()
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

    await asyncio.sleep(2)
    print(f"websocket send path={path}", flush=True)
    await ws.send_str('{"user_id":42}')

    async for msg in ws:
        if msg.type == WSMsgType.CLOSE:
            break

    return ws


app = web.Application()
app.router.add_get("/http/{name}", handle_http)
app.router.add_get("/ws/{name}", handle_ws)


async def start() -> None:
    runner = web.AppRunner(app)
    await runner.setup()

    http_site = web.TCPSite(runner, "0.0.0.0", 8080)
    tls_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    tls_context.load_cert_chain("/certs/node.pem", "/certs/node-key.pem")
    https_site = web.TCPSite(runner, "0.0.0.0", 8443, ssl_context=tls_context)

    await http_site.start()
    await https_site.start()

    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(start())
