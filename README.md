# EchoKV: A Distributed, Fault-Tolerant Key-Value Store in Go

EchoKV is a demonstration of a highly available, persistent key-value store built on Go. It utilizes the HashiCorp Raft consensus protocol to ensure strong consistency and fault tolerance across a cluster of nodes. This project showcases expertise in concurrency, networking, and durable state management.

## 🛠️ Core Technologies and Architecture

The system is built around the fundamental principles of distributed consensus .

| Component | Technology | Role |
| :--- | :--- | :--- |
| **Consensus** | HashiCorp Raft (`hashicorp/raft`) | Guarantees data consistency and handles leader election/failover. |
| **Durable Storage** | BoltDB (`go.etcd.io/bbolt`) | Provides persistence for the Raft replicated log and metadata. |
| **State Machine (FSM)** | In-Memory Map | The quick-access key-value store; updated only by committed Raft logs. |
| **Networking** | Go's `net/tcp` | Inter-node Raft RPCs and client-server communication. |

---

## 🚀 Setup and Running the Cluster

### 1. Build the Executables

Navigate to the project root directory and build the server and client binaries:

```bash
# Build the core server binary
go build -o echokv-server ./cmd/echokv-server

# Build the simple CLI client binary
go build -o echokv-cli ./cmd/echokv-cli
````

### 2\. Clean Environment

Ensure a clean slate by removing previous persistent data before starting a new cluster:

```bash
rm -rf data
```

### 3\. Start the Cluster (3-Node Example)

Start each node in a separate terminal. Followers automatically initiate the join process upon startup using the `--join-addr` flag.

| Node | Role | Command | Client Address | Raft Address |
| :--- | :--- | :--- | :--- | :--- |
| **Node 1** | **Leader (Bootstrap)** | `./echokv-server --node-id node-1 --listen-addr :5379 --raft-addr 127.0.0.1:15379` | `:5379` | `127.0.0.1:15379` |
| **Node 2** | **Follower (Auto-Join)** | `./echokv-server --node-id node-2 --listen-addr :5380 --raft-addr 127.0.0.1:15380 --join-addr 127.0.0.1:5379` | `:5380` | `127.0.0.1:15380` |
| **Node 3** | **Follower (Auto-Join)** | `./echokv-server --node-id node-3 --listen-addr :5381 --raft-addr 127.0.0.1:15381 --join-addr 127.0.0.1:5379` | `:5381` | `127.0.0.1:15381` |

-----

## 💻 Client Interaction and Administration

### Supported Commands

The client supports basic $\text{KV}$ operations and one key administrative command:

* `SET <key> <value>`: Writes are forwarded to the Leader and replicated via Raft.
* `GET <key>`: Reads from the local node (Eventual Consistency).
* `REMOVE <node-id>`: **(Admin)** Removes a server from the Raft configuration (must be run on the current Leader).

### Example Session

```bash
# Connect to the Leader (Node 1)
./echokv-cli --connect 127.0.0.1:5379

> SET db_status operational
OK

# Verify the write by querying a follower (Node 3)
./echokv-cli --connect 127.0.0.1:5381
> GET db_status
operational
```

-----

## 🛡️ Validation: Fault Tolerance Test

The final proof of concept is validating system integrity after a catastrophic failure.

1.  **Write Checkpoint Data:** Use the $\text{CLI}$ to set a unique key:

    ```bash
    ./echokv-cli --connect 127.0.0.1:5379
    > SET commit_point 1024
    OK
    ```

2.  **Crash the Leader:** Go to the terminal running **Node 1** and press **Ctrl+C** to simulate an abrupt failure.

3.  **Wait for Failover:** Wait 5-10 seconds. The remaining nodes (Node 2 and Node 3) will automatically elect a new Leader (e.g., Node 2).

4.  **Verify Data Integrity:** Connect to the **new Leader** (Node 2) and retrieve the data written before the crash:

    ```bash
    ./echokv-cli --connect 127.0.0.1:5380  # Connect to Node 2 (new Leader)
    > GET commit_point
    1024
    ```

    **Result:** Successful retrieval confirms that the $\text{Raft}$ log was persisted on the followers and successfully replayed to the new Leader's state machine.

5.  **Clean up Failed Node (Admin Action):** To prevent the new Leader from continuously retrying to replicate to the dead Node 1, explicitly remove it:

    ```bash
    ./echokv-cli --connect 127.0.0.1:5380 # Connect to the new Leader
    > REMOVE node-1
    OK
    ```

<!-- end list -->
