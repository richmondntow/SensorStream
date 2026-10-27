#!/usr/bin/env python3
"""Measure end-to-end request latency including TCP connection setup."""
import argparse
import concurrent.futures
import math
import threading
import time
from simulator import request


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--host', default='127.0.0.1')
    p.add_argument('--port', type=int, default=9000)
    p.add_argument('--clients', type=int, default=10)
    p.add_argument('--requests', type=int, default=100, help='requests per client')
    a = p.parse_args()
    if a.clients < 1 or a.requests < 1:
        p.error('clients and requests must be positive')
    barrier = threading.Barrier(a.clients)
    def client(i):
        durations, failures = [], 0
        barrier.wait()
        for _ in range(a.requests):
            start = time.perf_counter()
            try:
                result = request(a.host, a.port, f'PUT bench-{i} 25 80')
                if result != 'OK':
                    raise RuntimeError(result)
                durations.append((time.perf_counter() - start) * 1000)
            except (OSError, RuntimeError):
                failures += 1
        return durations, failures
    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=a.clients) as pool:
        results = list(pool.map(client, range(a.clients)))
    elapsed = time.perf_counter() - started
    values = sorted(v for times, _ in results for v in times)
    failures = sum(f for _, f in results)
    print(f'clients={a.clients} successful={len(values)} failed={failures} elapsed={elapsed:.3f}s throughput={len(values)/elapsed:.1f} requests/s')
    if values:
        print(f'latency_ms mean={sum(values)/len(values):.3f} p50={values[math.ceil(.50*len(values))-1]:.3f} p95={values[math.ceil(.95*len(values))-1]:.3f} p99={values[math.ceil(.99*len(values))-1]:.3f}')
    if failures:
        raise SystemExit(1)

if __name__ == '__main__':
    main()
