import socket
import threading

HOST = "127.0.0.1"
PORT = 5555

# Stores connected clients: {socket: name}
clients = {}
clients_lock = threading.Lock()


class LineReader:
    """Buffers raw bytes from a socket and yields complete, newline-delimited
    messages. This avoids the classic TCP framing bug where a single recv()
    call might return part of a message, multiple messages stuck together,
    or a message split across several recv() calls.
    """

    def __init__(self, sock, bufsize=4096):
        self.sock = sock
        self.bufsize = bufsize
        self.buffer = b""

    def read_line(self):
        """Block until one full newline-terminated message is available.
        Returns the decoded, stripped message, or None if the peer closed
        the connection.
        """
        while b"\n" not in self.buffer:
            chunk = self.sock.recv(self.bufsize)
            if not chunk:
                # Peer closed the connection. If there's a trailing partial
                # message with no newline, treat it as the final message;
                # otherwise signal closure.
                if self.buffer:
                    leftover, self.buffer = self.buffer, b""
                    return self._decode(leftover)
                return None
            self.buffer += chunk

        line, _, self.buffer = self.buffer.partition(b"\n")
        return self._decode(line)

    @staticmethod
    def _decode(raw_bytes):
        # Malformed/non-UTF-8 input is replaced rather than raising, so a
        # single bad client can't crash its handler thread.
        return raw_bytes.decode("utf-8", errors="replace").strip()


def broadcast(message, exclude_socket=None):
    """Send a newline-terminated message to all connected clients except the
    excluded one. Callers pass the message without a trailing newline.
    """
    framed = (message + "\n").encode("utf-8")
    with clients_lock:
        dead_sockets = []
        for client_socket in clients:
            if client_socket is exclude_socket:
                continue
            try:
                client_socket.sendall(framed)
            except (ConnectionError, OSError):
                dead_sockets.append(client_socket)

        # Clean up any sockets that failed to send
        for dead in dead_sockets:
            name = clients.pop(dead, "Unknown")
            print(f"{name} left (connection lost)")


def handle_client(client_socket, address):
    name = None
    reader = LineReader(client_socket)
    try:
        # First line from the client is treated as their chosen name
        name_line = reader.read_line()
        if name_line is None:
            client_socket.close()
            return

        name = name_line or f"Client{address[1]}"

        with clients_lock:
            clients[client_socket] = name

        print("Client connected")
        print(f"{name} joined")
        broadcast(f"*** {name} joined the chat ***")

        while True:
            message = reader.read_line()
            if message is None:
                break  # client disconnected

            if not message:
                continue  # ignore blank lines

            # Client sends "[HH:MM] text" — move the timestamp to the front
            # of the line so it reads "[HH:MM] NAME: text" instead of
            # "NAME: [HH:MM] text". Guard against malformed input (e.g. a
            # message that starts with '[' but has no closing '] ').
            if message.startswith("[") and "] " in message:
                timestamp, _, rest = message.partition("] ")
                formatted = f"{timestamp}] {name}: {rest}"
            else:
                formatted = f"{name}: {message}"

            print(formatted)
            broadcast(formatted)

    except (ConnectionError, OSError):
        pass
    finally:
        with clients_lock:
            clients.pop(client_socket, None)
        if name:
            print(f"{name} left")
            broadcast(f"*** {name} left the chat ***")
        client_socket.close()


def start_server():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((HOST, PORT))
    server_socket.listen()

    print("Server started...")
    print(f"Listening on {HOST}:{PORT}")

    try:
        while True:
            try:
                client_socket, address = server_socket.accept()
            except OSError as exc:
                print(f"Accept failed: {exc}")
                continue
            thread = threading.Thread(
                target=handle_client, args=(client_socket, address), daemon=True
            )
            thread.start()
    except KeyboardInterrupt:
        print("\nShutting down server...")
    finally:
        server_socket.close()


if __name__ == "__main__":
    start_server()
