# Concurrent Multi-Client Chat Server

## Overview

A terminal-based real-time chat application built using Python sockets and multithreading.
The system enables multiple clients to connect simultaneously to a central server and exchange messages in real time.

The project implements newline-delimited message framing to correctly handle TCP packet boundaries and supports concurrent communication through threaded client handlers.

---

## Features

* Real-time messaging between multiple clients
* Concurrent handling of multiple users using threads
* TCP socket-based client-server architecture
* Username-based messaging
* Join and leave notifications
* Timestamped chat messages
* Reliable newline-based message framing
* Graceful client disconnect handling
* UTF-8 safe message decoding

---

## Architecture


+-----------+
| Client 1  |
+-----------+
      |
+-----------+
| Client 2  |
+-----------+
      |
      v
+-------------------+
|   Chat Server     |
| Thread per Client |
+-------------------+
      ^
      |
+-----------+
| Client 3  |
+-----------+
```

---

## Tech Stack

* Python
* Socket Programming
* Multithreading
* TCP Networking

---

## Project Structure


distributed-chat-server/
│
├── server.py
├── client.py
├── README.md
├── screenshots/
└── requirements.txt
```

---

## How to Run

### Start Server

```bash
python server.py
```

Expected:

Server started...
Listening on 127.0.0.1:5555


---

### Start Clients

Open multiple terminals:

```bash
python client.py
```

Enter usernames and begin chatting.

---

## Example Interaction

Server:

```text
VIDUSHI joined
TEST joined
```

Client:

```text
[22:40] VIDUSHI: hello
[22:41] TEST: hi
```

---

## Challenges Solved

* Handling multiple simultaneous connections
* Preventing TCP message fragmentation issues
* Designing thread-safe shared client management
* Maintaining responsive real-time communication

---

## Future Improvements

* Chat rooms
* Message persistence
* Encryption
* Distributed multi-server architecture
* GUI interface

---

## Learnings

This project strengthened understanding of:

* Socket programming
* Concurrency
* Client-server communication
* TCP behavior
* Thread synchronization
