# gossip-protocol

Peer-to-peer failure detector implementing epidemic gossip over UDP. Nodes gossip heartbeats to random neighbours, merge membership lists, and independently detect failures — no central coordinator, no shared state.

## Quick Start

```bash
python main.py
```

Change `N` at the top of the file to run with any number of nodes.

## How it works

Each node runs 4 independent threads:
- **Heartbeat** — increments own counter every second
- **Sender** — gossips membership list to a random neighbour every 2 seconds over UDP
- **Receiver** — listens for incoming gossip and merges it
- **Watchdog** — marks nodes SUSPECTED after 10s of staleness, DEAD after 20s

## Performance

Sub-15s failure detection across 6 nodes with 2s gossip interval. Converges in O(log N) rounds.

## Key Concepts

- State is external — no node declares itself dead
- Local clocks only — `last_updated` is never trusted from the network
- Deep copy before send — snapshots isolated from live state
- UDP fire and forget — no handshake overhead

## Blog Post

*Link coming soon*