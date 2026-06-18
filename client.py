import socket
import threading
import sys
import time
import json
import uuid

HOST = input("Server IP (default 127.0.0.1): ") or "127.0.0.1"

port = input("Server port (5555/5556): ")
PORT = int(port) if port else 5555

class LineReader:
    """Buffers raw bytes from a socket and yields complete, newline-delimited
    messages, mirroring the server's framing so partial/coalesced TCP reads
    can't be misinterpreted as separate or merged messages.
    """

    def __init__(self, sock, bufsize=4096):
        self.sock = sock
        self.bufsize = bufsize
        self.buffer = b""

    def read_line(self):
        while b"\n" not in self.buffer:
            chunk = self.sock.recv(self.bufsize)
            if not chunk:
                if self.buffer:
                    leftover, self.buffer = self.buffer, b""
                    return self._decode(leftover)
                return None
            self.buffer += chunk

        line, _, self.buffer = self.buffer.partition(b"\n")
        return self._decode(line)

    @staticmethod
    def _decode(raw_bytes):
        return raw_bytes.decode("utf-8", errors="replace").strip()


def receive_messages(sock):
    reader = LineReader(sock)
    while True:
        try:
            line = reader.read_line()
            if line is None:
                print("Disconnected from server.")
                break
            print("\r" + line)
        except (ConnectionError, OSError):
            print("\nConnection closed.")
            break


def start_client():
    name = input("Enter your name: ").strip() or "Anonymous"

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((HOST, PORT))
    except ConnectionRefusedError:
        print("Server is not running.")
        return

    sock.sendall((name + "\n").encode("utf-8"))

    receiver = threading.Thread(target=receive_messages, args=(sock,), daemon=True)
    receiver.start()

    print(f"Connected as {name}. Type messages and press Enter to send. Ctrl+C to quit.")
    try:
        while True:
            message = input()
            if message:
                msg = {
                    "id": str(uuid.uuid4()),
                    "from": name,
                    "text": message,
                    "node": "CLIENT"
                  }

                sock.sendall((json.dumps(msg) + "\n").encode("utf-8"))
    except (KeyboardInterrupt, EOFError):
        print("\nClosing connection...")
    finally:
        sock.close()
        sys.exit(0)


if __name__ == "__main__":
    start_client()
