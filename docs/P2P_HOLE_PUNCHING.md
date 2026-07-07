# OVS P2P Hole Punching

Server-coordinated UDP hole punching for the OVS rollback stack. This document
covers the design, the exact wire protocol, the client (ASI) integration
contract, the backend changes required, and a staged rollout plan.

> **Status:** the **server side** (this repo, `holepunch` branch) is implemented
> and builds. It is **off by default** and cannot affect classic matches. The
> **client ASI** and **backend config** halves are specified here but not yet
> implemented — see [Scope & status](#scope--status).

---

## 1. Why

Today every match runs through a dedicated, frame-authoritative rollback server
in one cloud region. Each player's input travels:

```
player A  ──►  cloud server  ──►  player B
```

The server is the authority: it collects inputs, predicts missing ones,
calculates rift, and broadcasts the merged input stream back. That relay hop is
fine when players are near the region, but for two players who are close to
each other and far from the region (e.g. both in South America, server in
us-east) it roughly **doubles** the latency they feel versus a direct path:

```
A⇄server ≈ 120 ms,  B⇄server ≈ 120 ms   →  A⇄B felt ≈ 240 ms
A⇄B direct ≈ 30 ms                        →  A⇄B felt ≈ 30 ms
```

Hole punching lets the two clients open a **direct** UDP path through their home
NATs, so inputs flow peer-to-peer at the direct RTT. The cloud server steps out
of the input hot path and becomes a **coordinator** (rendezvous / STUN) plus a
**fallback relay** (TURN) for the ~10–20 % of pairs behind symmetric NATs where
a direct hole can't be opened.

---

## 2. Architecture

```
                         ┌─────────────────────────────┐
                         │      OVS Cloud Server        │
                         │  (this repo)                 │
                         │                              │
   Register / KeepAlive  │  P2PCoordinator              │
   PunchResult / Relay   │   • learns reflexive endpts  │
   ┌────────────────────►│   • elects host              │
   │                     │   • issues synchronized      │
   │   PeerList/PunchNow  │     PunchNow                 │
   │   UseDirect/UseRelay │   • relays (TURN) on failure │
   │◄────────────────────│   • authority fallback stays │
   │                     └─────────────────────────────┘
   │                                    ▲
   │   ┌───────────────┐  direct UDP    │  control channel
   │   │  Client A ASI │◄──────────────►│  (same socket)
   └──►│  local proxy  │   (punched)    │
       │  game↔127.0.0.1│               ▼
       └───────────────┘        ┌───────────────┐
                                │  Client B ASI │
                                │  local proxy  │
                                └───────────────┘
```

The game client speaks a **fixed** UDP rollback protocol to a single endpoint
and is server-authoritative (it trusts the server's input stream and rift). We
do not change the game's netcode. Instead the **ASI runs a tiny local UDP
proxy**: the game connects to `127.0.0.1:<port>` as if that were the rollback
server; the proxy forwards game traffic to whichever path is live:

- **Direct** to the peer's punched endpoint (low latency), or
- **Relay** through the cloud server (fallback), or
- **Authority** — the classic path, if coordination is skipped/fails.

Because the client is server-authoritative, one endpoint must be the authority.
In P2P mode that is the elected **host** peer; guests connect to the host. This
is the same host-authoritative shape the game itself used for P2P matches (the
`is_host` field already exists in the match config).

---

## 3. Control channel

P2P control packets share the game's UDP socket but are framed so they can never
be confused with a game rollback frame (which is bitmask-compressed and begins
with a compression mask byte).

### 3.1 Framing

```
offset  size  field
0       4     Magic = 'O''V''S''P'  (0x4F 0x56 0x53 0x50)
4       1     Version = 0x01
5       1     Subtype (see below)
6       …     Subtype payload
```

All integers **little-endian**. Strings are length-prefixed ASCII (`u8 len`
then bytes). Control packets are **never** bitmask-compressed.

The receiver checks `Magic + Version + a known Subtype` before attempting game
decompression. Residual collision risk (a compressed game frame that happens to
begin with these exact 6 bytes) is astronomically small and, if it ever
occurred, would drop a single frame the rollback layer already tolerates.

### 3.2 Subtypes

| Dir | Value | Name          | Payload |
|-----|-------|---------------|---------|
| C→S | 0x01  | `Register`    | `u16 playerIndex`, `str8 matchId`, `str8 key` |
| C→S | 0x02  | `PunchResult` | `u16 playerIndex`, `u16 peerIndex`, `u8 success` |
| C→S | 0x03  | `RelayData`   | `u16 srcIndex`, `u16 dstIndex`, `u16 len`, `len bytes` |
| C→S | 0x04  | `KeepAlive`   | `u16 playerIndex` |
| S→C | 0x81  | `RegisterAck` | `u8 role`, `u8 mode`, `str8 publicIp`, `u16 publicPort` |
| S→C | 0x82  | `PeerList`    | `u8 count`, then per peer: `u16 index`, `u8 role`, `str8 ip`, `u16 port` |
| S→C | 0x83  | `PunchNow`    | `u16 startDelayMs`, `u8 attempts`, `u16 intervalMs` |
| S→C | 0x84  | `RelayDeliver`| `u16 srcIndex`, `u16 len`, `len bytes` |
| S→C | 0x85  | `UseRelay`    | `u16 peerIndex` |
| S→C | 0x86  | `UseDirect`   | `u16 peerIndex`, `str8 ip`, `u16 port` |

`role`: `0 = Guest`, `1 = Host`. `mode`: `0 = Off`, `1 = Preferred`, `2 = Forced`.

The canonical encoder/decoder lives in
[`P2P/P2PControl.cs`](../OVSRollbackServer/P2P/P2PControl.cs) — treat that file
as the source of truth and mirror it byte-for-byte on the client.

---

## 4. Coordination state machine (server)

Implemented in [`P2P/P2PCoordinator.cs`](../OVSRollbackServer/P2P/P2PCoordinator.cs).

1. **Registering** — each client sends `Register`. The server records the
   reflexive (server-observed) `IP:port`, replies `RegisterAck` (STUN echo +
   assigned role), and waits until every expected human peer has registered, or
   `RegistrationTimeoutMs` elapses with ≥ 2 present.

2. **Punching** — the server elects a host (config `is_host`, else lowest
   registered `playerIndex`), broadcasts `PeerList` to everyone, and issues a
   `PunchNow` with a common `startDelayMs` so all clients fire punches at the
   same instant. Clients punch each peer `attempts` times, `intervalMs` apart,
   and report each direction with `PunchResult`.

3. A pair `(a,b)` is **Direct** only when **both** `a→b` and `b→a` are confirmed
   (a NAT hole must be open both ways). The server then sends each side
   `UseDirect` with the other's endpoint.

4. Pairs still unconfirmed when `PunchWindowMs` closes get `UseRelay` (server
   forwards their traffic via `RelayData`/`RelayDeliver`), unless the match is
   `Forced`, in which case they fall back to the authority path.

5. **Established** once every pair is resolved. **FellBack** if registration
   never reaches 2 peers — the match just runs on the classic authority path.

Liveness: a session is evicted after every peer is silent for
`PeerLivenessTimeoutMs`; `EndMatch(matchId)` also clears it when the authority
match ends.

---

## 5. Client (ASI) integration contract

The ASI already talks to the OVS backend and the rollback server. Adding P2P is
four pieces:

### 5.1 Local proxy

Bind a local UDP socket (e.g. `127.0.0.1:<gamePort>`), point the game's rollback
target at it (the ASI already rewrites the server endpoint the game uses).
Maintain, per peer `playerIndex`, a `Route { Direct(ep) | Relay | Authority }`.

- **Game → proxy**: forward to the current route:
  - `Direct`: `SendTo(peerEndpoint, gameBytes)` (unchanged game bytes).
  - `Relay`: wrap in `RelayData{ src=self, dst=peer, gameBytes }` → cloud.
  - `Authority`: forward straight to the cloud rollback server (today's behavior).
- **Peer/relay → proxy**: unwrap and `SendTo(127.0.0.1:gamePort, gameBytes)` so
  the game sees it as normal server traffic.

### 5.2 Control loop (on the socket that faces the cloud server)

1. On match start, if the backend flagged the match P2P-enabled, send `Register`
   (with the same `matchId`/`key` the game uses). Repeat every ~1 s until
   `RegisterAck`. Read your public endpoint from the ack (useful for logging).
2. On `PeerList`, store peers' reflexive endpoints and the host role.
3. On `PunchNow`, wait `startDelayMs`, then fire `attempts` empty/again small
   UDP datagrams to each peer's reflexive endpoint, `intervalMs` apart. When a
   datagram is **received** from a peer's endpoint, mark that direction open and
   send `PunchResult{ peer, success=1 }`. (Send `success=0` never; just stop.)
4. On `UseDirect{ peer, ep }`, set that peer's route to `Direct(ep)` and start
   using the punched path. On `UseRelay{ peer }`, set route to `Relay`.
5. Send `KeepAlive` every ~2 s for the whole match to keep the NAT binding (and
   the relay mapping) alive.

### 5.3 Host vs guest

- **Host**: runs the authority. In the simplest first version the host keeps
  using the **cloud rollback server as its authority** and only *guests* switch
  to a direct/relay path to the host's proxy — i.e. P2P shortens the guest↔host
  leg. A later version can move the authority fully onto the host's machine
  (running this server binary locally, as the old C++ `p2p` branch attempted).
- **Guest**: routes its rollback traffic to the host via Direct or Relay.

### 5.4 Reference: old C++ proxy

The archived `p2p` branch of the C++ server (`src/rollback_server.cpp`:
`initiateUdpHolePunching`, `forwardToHost`, `forwardToLocal`, proxy-mode
receive) is a working sketch of the localhost↔peer forwarding and the punch
loop. Reuse its structure; replace its ad-hoc `'PUNCH'` marker and hard-coded
`41234` with this document's control protocol and the server-provided endpoints.

---

## 6. Backend (OVS HTTP) changes

The rollback server pulls match config from the OVS backend. Two additions:

1. **`p2p_mode`** on the match config object: `0` Off (default), `1` Preferred,
   `2` Forced. Start by setting `1` only for 1v1 ranked between two humans.
2. **`is_host`** per player (field already consumed here). Pick the host
   deterministically — e.g. the lower Elo, the better-connected region, or just
   `player_index == 0`. The server falls back to lowest `player_index` if none
   is flagged.

No other backend changes are required for the coordinator; the server continues
to fetch config exactly as today.

---

## 7. Server configuration

`appsettings.json` → `P2P` block, or `P2P__*` env vars (same override scheme as
every other section). All off unless `Enabled=true`.

| Key | Default | Meaning |
|-----|---------|---------|
| `Enabled` | `false` | Master switch — coordinator isn't even constructed when false |
| `RegistrationTimeoutMs` | `4000` | Wait for all peers before proceeding with ≥ 2 |
| `PunchWindowMs` | `3000` | Wait for punch confirmations before relaying unresolved pairs |
| `PunchAttempts` | `8` | Punch datagrams per peer |
| `PunchIntervalMs` | `100` | Gap between punch datagrams |
| `PunchStartDelayMs` | `250` | Lead time so clients punch simultaneously |
| `PeerLivenessTimeoutMs` | `15000` | Evict a session after this much peer silence |
| `RelayEnabled` | `true` | Act as TURN relay when a direct hole fails |

---

## 8. Rollout plan

1. **Server merge (this branch).** Off by default. No behavior change. Deploy so
   the coordinator code is live but dormant.
2. **Client proxy + control loop.** Ship an ASI that can register, punch, and
   route, but keep the backend flag off — nothing engages yet.
3. **Shadow test.** Set `p2p_mode=1` for a whitelist of consenting testers.
   Compare felt latency (direct vs. authority) and punch success rate from the
   `[P2P]` server logs. Verify relay fallback on a known symmetric-NAT tester.
4. **1v1 ranked, region-gated.** Enable `p2p_mode=1` for 1v1s where both players
   are far from the serving region and near each other (the case with the
   biggest win). Keep authority fallback on.
5. **Broaden.** 2v2 and wider once 1v1 is stable. Consider host-local authority
   as a follow-up once the guest↔host shortening is proven.

At every stage, `p2p_mode=0` (or `Enabled=false`) is a total, instant kill
switch back to today's dedicated-server behavior.

---

## 9. Edge cases & notes

- **Symmetric NAT.** Punching fails; the pair is relayed (or, in `Forced`,
  stays on authority). This is expected for a minority of players.
- **NAT rebind mid-match.** `KeepAlive` refreshes the mapping; a hard rebind
  requires a fresh `Register`. Until then that peer relays.
- **Spectators / bots.** Never punch — they have no home-NAT ASI. The server
  excludes `is_spectator`, `player_index >= 8888`, and `is_bot` from the
  expected-peer count.
- **Authority always available.** The dedicated tick loop is untouched. If P2P
  coordination produces nothing, the match is exactly as it is today.
- **Cheating surface.** Host-authoritative P2P moves trust toward a client. The
  first rollout keeps the cloud as authority and only shortens the guest leg,
  which preserves the current trust model; full host authority is a later,
  separately-reviewed step.

---

## 10. Scope & status

**Done (server, this branch):**
- Control protocol codec (`P2P/P2PControl.cs`).
- Coordinator state machine: register, host election, synchronized punch,
  pair resolution, relay fallback, liveness/eviction (`P2P/P2PCoordinator.cs`).
- Magic-prefixed intercept on the existing UDP socket; zero impact on the
  authority hot path (`Core/RollbackServer.cs`).
- Config plumbing + `appsettings.json` (off by default).

**Not done (tracked here, separate work):**
- Client ASI local proxy + control loop (Section 5).
- Backend `p2p_mode` emission (Section 6).
- Optional host-local authority (Section 5.3).
- End-to-end latency/success telemetry dashboards.
