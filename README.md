# SensorStream

A C++17 TCP server that collects the latest temperature and battery readings from concurrent simulated devices. A fixed worker pool handles connections, a bounded synchronized queue holds accepted sockets, and a mutex protects the in-memory readings. Python utilities simulate devices and measure request latency and throughput.

## Requirements

- macOS or Linux with a C++17 compiler and POSIX sockets
- CMake 3.16 or newer and Python 3

## Build and run

```sh
cmake -S . -B build
cmake --build build
ctest --test-dir build --output-on-failure
./build/sensorstream_server 9000 4
```

In a second terminal:

```sh
python3 simulator.py --devices 5 --updates 3
python3 benchmark.py --clients 10 --requests 100
```

The server listens on `127.0.0.1` only. Stop it with Ctrl+C. The simulator opens one connection per update. `benchmark.py` reports successes, failures, throughput, mean and p50/p95/p99 latency; latency includes connection setup. Compare client counts on the same machine and record the machine, worker count and command with your results. Results are environment dependent; no benchmark figures are claimed here.

## Protocol

TCP is a byte stream, so each request and response ends in a newline. A connection can send multiple requests. Maximum request length is 512 bytes, excluding the newline.

- `PUT device-1 23.5 80` stores the latest temperature in Celsius (-100 to 200) and battery percentage (0 to 100); response: `OK`.
- `GET` returns `DATA count id,temperature,battery ...`, sorted by device ID.
- Malformed requests receive `ERR invalid request`.

IDs are up to 64 ASCII letters, digits, underscores or hyphens. Data lives only in memory and is lost when the server stops. Each worker serves a connection until it closes or is idle for one second. The queue holds up to 256 waiting connections; excess connections are closed. The server has no authentication or encryption and is intended for local learning and testing, not public deployment.

## Design

The main thread accepts sockets and puts them into a bounded queue. Worker threads take sockets from the queue and handle newline-delimited messages. A mutex protects the shared map, and a condition variable wakes workers when connections arrive. On SIGINT or SIGTERM, the listener stops, pending sockets close, and workers finish or time out before joining.

`tests/integration.py` checks concurrent updates, replacement, invalid messages, oversized input, disconnects and shutdown. GitHub Actions runs the build and tests on macOS and Linux.
