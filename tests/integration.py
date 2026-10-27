import concurrent.futures
import signal
import socket
import subprocess
import sys
import time


def exchange(port, data):
    with socket.create_connection(('127.0.0.1', port), timeout=3) as s:
        s.settimeout(3)
        s.sendall(data)
        return s.makefile('rb').readline().decode()


def main():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        port = s.getsockname()[1]
    server = subprocess.Popen([sys.argv[1], str(port), '4'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        for _ in range(100):
            try:
                with socket.create_connection(('127.0.0.1', port), timeout=.1):
                    break
            except OSError:
                if server.poll() is not None:
                    raise RuntimeError(server.stderr.read())
                time.sleep(.02)
        else:
            raise RuntimeError('server did not start')
        assert exchange(port, b'GET\n') == 'DATA 0\n'
        assert exchange(port, b'PUT x 20 101\n').startswith('ERR')
        assert exchange(port, b'PUT x 20 50 extra\n').startswith('ERR')
        assert exchange(port, b'x' * 513 + b'\n').startswith('ERR')
        assert exchange(port, b'PUT x 22 50\n') == 'OK\n'
        assert exchange(port, b'PUT x 23 40\n') == 'OK\n'
        assert exchange(port, b'GET\n') == 'DATA 1 x,23,40\n'
        def put(i):
            return exchange(port, f'PUT d{i} {i} 50\n'.encode())
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
            assert all(x == 'OK\n' for x in pool.map(put, range(40)))
        assert exchange(port, b'GET\n').startswith('DATA 41 ')
        with socket.create_connection(('127.0.0.1', port), timeout=2) as s:
            s.sendall(b'PUT partial')
        assert exchange(port, b'GET\n').startswith('DATA 41 ')
        server.send_signal(signal.SIGTERM)
        assert server.wait(timeout=4) == 0
        print('Integration tests passed')
    finally:
        if server.poll() is None:
            server.kill()
            server.wait()

if __name__ == '__main__':
    main()
