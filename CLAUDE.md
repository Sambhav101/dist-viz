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
  main.rs        # entry point (mostly empty)
  raft.rs        # Raft implementation
  paxos.rs       # Paxos implementation (not started)
  simulation.rs  # simulation runner (not started)
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
| 4 | Implement Raft leader heartbeats | **next — branch: issue-4-raft-heartbeats** |
| 5 | Add WebSocket server with axum | open |
| 6 | Frontend: node SVG and live state | open |
| 7 | Frontend: animated message arrows | open |
| 8 | Define Paxos message types and node state | open |
| 9 | Implement Basic Paxos protocol | open |
| 10 | Add interactive controls | open |

## Current state of raft.rs

- `NodeState` enum: `Follower`, `Candidate`, `Leader`
- `Message` enum: `RequestVote`, `RequestVoteReply`, `AppendEntries`, `AppendEntriesReply` — each with appropriate fields
- `Node` struct: `id`, `state`, `current_term`, `voted_for`, `cluster_size`, `votes_received`, `rx`
- `impl Node`:
  - `new(id, rx, cluster_size)` — constructor, starts as Follower
  - `start_election(&mut self)` — transitions to Candidate, increments term, votes for self
  - `handle_message(&mut self, msg)` — handles `RequestVote` (vote granting logic) and `RequestVoteReply` (majority check, Leader promotion)

## Next up: Issue #4 — Raft leader heartbeats

This introduces **async Rust**. Key concepts to cover:
- `async fn` — a function that can be paused while waiting
- `await` — pause here until this completes
- `tokio::select!` — wait for multiple async things at once, handle whichever fires first
- `tokio::time::interval` — fires repeatedly on a fixed schedule

A node needs to do two things simultaneously:
- Wait for incoming messages on `rx`
- Fire a timer (election timeout for followers, heartbeat interval for leaders)

That requires `tokio::select!` — it races multiple async operations and handles whichever completes first.
