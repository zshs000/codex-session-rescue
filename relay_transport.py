"""HTTP/SSE transport using the current proxy, without controlling proxy clients."""
import http.client
from urllib.parse import urlsplit, urlunsplit
from urllib.request import getproxies, proxy_bypass

import repair_encrypted_session as engine
from session_gateway import HOP_HEADERS


def validate_network(value):
    engine.require(isinstance(value, str), "网络出口格式无效。")
    if value in {"auto", "direct"}:
        return value
    parsed = urlsplit(value)
    engine.require(parsed.scheme == "http" and parsed.hostname and not parsed.username and
                   not parsed.password and parsed.path in {"", "/"} and not parsed.query and not parsed.fragment,
                   "代理请填写 HTTP 地址，例如 http://127.0.0.1:7897。")
    try:
        parsed.port
    except ValueError as error:
        raise engine.RepairError("代理端口无效。") from error
    return value.rstrip("/")


def selected_proxy(upstream, network):
    validate_network(network)
    if network == "direct":
        return None
    if network != "auto":
        return network
    if upstream.hostname in {"127.0.0.1", "localhost", "::1"} or proxy_bypass(upstream.hostname):
        return None
    value = getproxies().get(upstream.scheme)
    if value:
        validate_network(value)
    return value


def open_response(method, url, headers, raw, network="auto"):
    upstream = urlsplit(url)
    path = urlunsplit(("", "", upstream.path or "/", upstream.query, ""))
    proxy_value = selected_proxy(upstream, network)
    if proxy_value:
        proxy = urlsplit(proxy_value)
        if upstream.scheme == "https":
            connection = http.client.HTTPSConnection(proxy.hostname, proxy.port or 80, timeout=300)
            connection.set_tunnel(upstream.hostname, upstream.port or 443)
        else:
            connection = http.client.HTTPConnection(proxy.hostname, proxy.port or 80, timeout=300)
            path = url
    else:
        client = http.client.HTTPSConnection if upstream.scheme == "https" else http.client.HTTPConnection
        connection = client(upstream.hostname, upstream.port, timeout=300)
    try:
        connection.request(method, path, raw, headers)
        return connection, connection.getresponse()
    except BaseException:
        connection.close()
        raise


def incoming_headers(handler):
    hop = HOP_HEADERS | {key.strip().lower() for key in handler.headers.get("Connection", "").split(",")}
    headers = {key: value for key, value in handler.headers.items()
               if key.lower() not in hop | {"x-session-token"}}
    headers["Accept-Encoding"] = "identity"
    return headers


def send_response_headers(handler, response):
    handler.send_response(response.status)
    hop = HOP_HEADERS | {key.strip().lower() for key in response.getheader("Connection", "").split(",")}
    for key, value in response.getheaders():
        if key.lower() not in hop:
            handler.send_header(key, value)
    handler.send_header("Connection", "close")
    handler.end_headers()
    handler.close_connection = True


def send_response_body(handler, response):
    while True:
        chunk = response.read1(64 * 1024)
        if not chunk:
            return
        handler.wfile.write(chunk)
        handler.wfile.flush()
