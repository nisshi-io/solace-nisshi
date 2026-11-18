#!/usr/bin/env python3
"""A TCP load balancer for run.sh: accepts connections on one port and hands each to the next
backend in turn, so the clients of a benchmark spread over several brokers like they do behind a
layer 4 balancer. It copies bytes and knows nothing of Kafka.

usage: balance.py LISTEN_PORT BACKEND_PORT...
"""
import asyncio
import itertools
import sys


async def pipe(reader, writer):
    try:
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()
    except (ConnectionError, asyncio.CancelledError):
        pass
    finally:
        writer.close()


async def main(listen, backends):
    turn = itertools.cycle(backends)

    async def handle(client_reader, client_writer):
        try:
            backend_reader, backend_writer = await asyncio.open_connection("127.0.0.1", next(turn))
        except OSError:
            client_writer.close()
            return
        await asyncio.gather(
            pipe(client_reader, backend_writer), pipe(backend_reader, client_writer)
        )

    server = await asyncio.start_server(handle, "127.0.0.1", listen)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main(int(sys.argv[1]), [int(port) for port in sys.argv[2:]]))
