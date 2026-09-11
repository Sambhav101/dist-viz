# dist-viz

A distributed systems visualizer built in Rust. The goal is to simulate and visualize Raft and Paxos consensus algorithms in a browser.

## Purpose

This is a **learning project**. The user is learning both distributed systems concepts (Raft, Paxos) and the Rust language simultaneously by building this project from scratch. The role of Claude is to be a mentor/guide — teach the concepts behind each step, explain Rust syntax when needed, and let the user do the actual coding.

## Collaboration style

- User writes all the code. Do not write implementations unless explicitly asked.
- Explain *why* before *what* — distributed systems concept first, then what to write.
- Explain Rust syntax clearly when the user is unfamiliar with it.
- Make a git commit for every small meaningful step — keep commits tiny and focused.
- Keep explanations beginner-friendly.

## Project structure

```
src/
  main.rs        # #[tokio::main], declares modules, calls simulation::run_cluster(5)
  raft.rs        # Raft node: state, messages, handle_message, async run loop, unit tests
  paxos.rs       # Paxos implementation (not started)
  simulation.rs  # central hub: spawns nodes, routes Envelopes between them
  ws.rs          # WebSocket handler (not started)
static/
  index.html     # frontend (not started)
```

## GitHub issues

| # | Title | Status |
|---|-------|--------|
| 1 | Define Raft message types and node state enum | merged |
| 2 | Implement Raft node struct and mailbox | merged |
| 3 | Implement Raft leader election | merged |
| 4 | Implement Raft leader heartbeats | merged |
| 5 | Add WebSocket server with axum | **next — branch: issue-5-websocket** |
| 6 | Frontend: node SVG and live state | open |
| 7 | Frontend: animated message arrows | open |
| 8 | Define Paxos message types and node state | open |
| 9 | Implement Basic Paxos protocol | open |
| 10 | Add interactive controls | open |
| 15 | Bug: ignore stale RequestVoteReply | open |

## Architecture (decided in issue 4)

Nodes never talk to each other directly. Every message goes through a **central hub** in `simulation.rs`:

```
Node ──Envelope{from,to,msg}──► hub_rx ──► node_txs[to] ──► Node
```

- Each `Node` holds its own `rx: Receiver<Message>` (mailbox) and a clone of `tx: Sender<Envelope>` (into the hub).
- The hub owns `Vec<Sender<Message>>` indexed by node id and forwards each envelope. This one loop is where the WebSocket broadcast (#5) and kill/partition controls (#10) plug in.
- Each node runs as a tokio task via `Node::run(mut self)`: `tokio::select!` over the mailbox and a single `deadline` that is an election timeout (random 150–300 ms) for followers/candidates or a heartbeat interval (50 ms) for leaders.

## Current state of raft.rs

- `NodeState` (Copy, PartialEq): `Follower`, `Candidate`, `Leader`
- `Message` (Clone): `RequestVote`, `RequestVoteReply`, `AppendEntries`, `AppendEntriesReply`; `Message::term()` helper
- `Envelope { from, to, msg }`
- `Node`: `id`, `state`, `current_term`, `voted_for`, `votes_received`, `rx`, `tx`, `cluster_size`
  - `new`, `start_election` (broadcasts RequestVote), `handle_message` (step-down on higher term, replies to RequestVote and AppendEntries, majority promotion), `send_heartbeats`, `send_to`, `broadcast`, `run`
- Tests in `#[cfg(test)] mod tests`: vote once per term, step down on higher term, leader on majority
- `cargo run` prints a live 5-node election and heartbeats. `cargo test` passes 3 tests.

## Next up: Issue #5 — WebSocket server with axum

The hub loop already sees every message. #5 adds an axum server that serves `static/index.html` and, on each WebSocket connection, streams JSON events (node state changes and messages in flight) so the browser can draw them.

Gotcha learned in #4: a file in `src/` is not compiled until `main.rs` declares it with `mod name;`.
