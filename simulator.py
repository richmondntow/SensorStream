#!/usr/bin/env python3
"""Run concurrent simulated sensors; each request opens a short-lived TCP connection."""
import argparse
import concurrent.futures
import random
import socket
import time


def request(host, port, message):
    with socket.create_connection((host, port), timeout=5) as sock:
        sock.settimeout(5)
        sock.sendall((message + '\n').encode())
        with sock.makefile('r', encoding='ascii') as stream:
            response = stream.readline().strip()
            if not response:
                raise ConnectionError('server closed connection without a response')
            return response


def run_device(index, args):
    rng = random.Random(index)
    for _ in range(args.updates):
        response = request(args.host, args.port, f'PUT device-{index} {rng.uniform(18, 35):.2f} {rng.uniform(20, 100):.2f}')
        if response != 'OK':
            raise RuntimeError(response)
        if args.interval:
            time.sleep(args.interval)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=9000)
    parser.add_argument('--devices', type=int, default=5)
    parser.add_argument('--updates', type=int, default=3)
    parser.add_argument('--interval', type=float, default=0.2)
    args = parser.parse_args()
    if args.devices < 1 or args.updates < 1 or args.interval < 0:
        parser.error('devices and updates must be positive; interval must be nonnegative')
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.devices) as pool:
        list(pool.map(lambda i: run_device(i, args), range(args.devices)))
    print(request(args.host, args.port, 'GET'))

if __name__ == '__main__':
    main()
