import socket
import threading
import json
import uuid
import time
import os
import heapq

HOST = "127.0.0.1"
PORT = 5556

SERVER_NAME = "NODE-2"

PEER_HOST = "127.0.0.1"
PEER_PORT = 5555

# Stores connected clients: {socket: name}
clients = {}
clients_lock = threading.Lock()
seen_messages = set()
lamport_clock = 0
message_log = []

peer_socket = None
peer_lock = threading.Lock()

LOG_FILE = f"{SERVER_NAME}_messages.log"

def tick(clock):
    return clock + 1

def connect_to_peer():
    global peer_socket
    try:
        peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peer_socket.connect((PEER_HOST, PEER_PORT))
        print("Connected to peer node")
    except:
        peer_socket = None


def load_history():
    if not os.path.exists(LOG_FILE):
        return

    print("\n--- REPLAYING HISTORY ---\n")

    try:
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    msg = json.loads(line.strip())

                    formatted = f"[L{msg.get('lamport','?')}] [{msg['node']}] {msg['from']}: {msg['text']}"
                    print(formatted)

                except:
                    continue
    except Exception as e:
        print("History load failed:", e)

    print("\n--- END HISTORY ---\n")

def request_sync():
    try:
        peer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peer.connect((PEER_HOST, PEER_PORT))
        peer.sendall(("SYNC_REQUEST\n").encode("utf-8"))
        peer.close()
    except:
        pass

request_sync()

def create_message(name, text, node):
    return {
        "id": str(uuid.uuid4()),
        "from": name,
        "text": text,
        "node": node,
        "timestamp": time.time()
    }

def send_to_peer(message):
    global peer_socket

    if peer_socket is None:
        return

    try:
        with peer_lock:
            peer_socket.sendall(("PEER|" + message + "\n").encode("utf-8"))
    except:
        peer_socket = None

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


        pass

def handle_client(client_socket, address):
    name = None
    reader = LineReader(client_socket)
    try :
        # First line from the client is treated as their chosen name
        name_line = reader.read_line()

        if name_line and name_line.startswith("PEER|"):
           forwarded = name_line[5:]
           print(forwarded)
           broadcast(forwarded)
           client_socket.close()
           return
        
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
              break

           if not message:
              continue


           if message == "SYNC_REQUEST":
               try:
                   with open(LOG_FILE, "r", encoding="utf-8") as f:
                        for line in f:
                           peer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                           peer.connect((PEER_HOST, PEER_PORT))
                           peer.sendall(("SYNC_DATA|" + line.strip() + "\n").encode("utf-8"))
                           peer.close()
               except:
                     pass
               continue


           if message.startswith("PEER|"):
              message = message[5:]

    
           if message.startswith("SYNC_DATA|"):
              raw = message[11:]

              try:
                msg = json.loads(raw)
              except:
                continue

              if msg["id"] in seen_messages:
                 continue
              seen_messages.add(msg["id"])

              formatted = f"[SYNC][L{msg.get('lamport','?')}] [{msg['node']}] {msg['from']}: {msg['text']}"
              print(formatted)

              broadcast(json.dumps(msg))
              continue

    
           try:
               msg = json.loads(message)
           except:
             continue

           if msg["id"] in seen_messages:
             continue
           seen_messages.add(msg["id"])

           # lamport update
           global lamport_clock
           lamport_clock = tick(lamport_clock)
           msg["lamport"] = lamport_clock

           # persistence
           with open(LOG_FILE, "a", encoding="utf-8") as f:
             f.write(json.dumps(msg) + "\n")

           # update node
           msg["node"] = SERVER_NAME

           # local log
           message_log.append(msg)
           message_log.sort(key=lambda x: x["lamport"])

           # print
           formatted = f"[L{msg['lamport']}] [{msg['node']}] {msg['from']}: {msg['text']}"
           print(formatted)

           # propagate
           broadcast(json.dumps(msg))
           send_to_peer(json.dumps(msg))

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
    load_history()
    connect_to_peer()

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
