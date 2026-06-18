#  Mini Kafka Chat System (Distributed Event-Driven Messaging)

A lightweight distributed messaging system inspired by Kafka architecture, built using Python sockets.  
It demonstrates core distributed systems principles such as log replication, event streaming, causal ordering, and fault-tolerant recovery.

---

## 🚀 Key Features

-  Multi-node distributed architecture (Node-1 ↔ Node-2)
-  Event-driven message propagation (like a mini message broker)
-  Log replication across nodes (Kafka-like behavior)
-  Lamport Logical Clocks for causal ordering
-  Persistent event logs (append-only storage)
-  Crash recovery using log replay
-  Node synchronization (SYNC_REQUEST / SYNC_DATA protocol)
-  Deduplication using unique event IDs

---

##  System Architecture
    ┌──────────────┐
    │   Client A   │
    └──────┬───────┘
           │
    ┌──────▼───────┐
    │   NODE-1     │◄──────────────┐
    │ (Broker)     │               │ Event Replication
    └──────┬───────┘               │
           │                       │
    ┌──────▼───────┐               │
    │   NODE-2     │───────────────┘
    │ (Broker)     │
    └──────┬───────┘
           │
    ┌──────▼───────┐
    │   Client B   │
    └──────────────┘

    
---

## ⚙️ How It Works

1. Client sends a message (event) to a node
2. Node assigns a Lamport timestamp
3. Event is appended to persistent log
4. Event is broadcast to local clients
5. Event is replicated to peer node
6. Peer node deduplicates and processes event
7. On restart, logs are replayed to rebuild state
8. Nodes synchronize missing events using sync protocol

---

## 🔁 Fault Tolerance

This system ensures reliability via:

-  Append-only event logs (Kafka-like storage model)
-  Log replay for state recovery after crash
-  Cross-node replication of events
-  Deduplication using unique event IDs
-  Synchronization protocol for consistency recovery

---

## 🧠 Concepts Demonstrated

- Distributed Systems Fundamentals
- Event Streaming Architecture (Kafka-inspired)
- Log-based Storage Model
- Eventual Consistency
- Lamport Logical Clocks
- Peer-to-Peer Replication
- Fault Tolerance & Recovery

---

## 📦 Project Structure
mini-kafka-chat-system/
│
├── server.py # NODE-1 (broker)
├── server_node.py # NODE-2 (broker)
├── client.py # chat client
├── requirements.txt
├── NODE-1_messages.log
├── NODE-2_messages.log
└── README.md


---

## 🛠️ Tech Stack

- Python 3
- Socket Programming (TCP)
- Multithreading
- JSON-based event format
- File-based persistence (event log storage)

---

## 🚀 Future Improvements

-  Partitioned event streams (Kafka partitions simulation)
-  Vector clocks (stronger causality model)
-  Leader-based replication (Raft-style consensus)
-  Docker-based multi-node deployment
-  Web UI for real-time event streaming

---

##  Author

Vidushi Singh

---

## ⭐ Why This Project Matters

This project simulates a simplified version of a distributed event streaming system similar to Kafka.

It demonstrates:
- Event-driven architecture thinking
- Distributed replication mechanisms
- Fault-tolerant log-based systems
- Core system design principles used in large-scale backend systems
