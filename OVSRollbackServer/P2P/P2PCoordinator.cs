// P2PCoordinator.cs
//
// The server-side brain of the hole-punching layer. One coordinator instance
// lives on the rollback server and manages a coordination session per match.
//
// Flow (see docs/P2P_HOLE_PUNCHING.md for the full picture):
//   0. The rollback server calls EnableMatch(matchId, info) when it creates a
//      match (it already has the fetched config in hand). This is the ONLY
//      place match config enters the coordinator — the UDP receive/control path
//      never performs a blocking config fetch, so it cannot stall the
//      frame-authoritative hot path.
//   1. Each match player's ASI sends a Register control packet. The server
//      records the reflexive (public) endpoint it observed. Registrations that
//      arrive before EnableMatch are briefly buffered and drained once the
//      match is enabled.
//   2. Once every expected participant has registered (or a short timeout
//      elapses with at least two present), the coordinator elects a host,
//      broadcasts the full PeerList, and issues a synchronized PunchNow so all
//      clients fire UDP punches at the same moment.
//   3. Clients report PunchResult per peer. When BOTH directions of a pair are
//      confirmed, the coordinator tells each side to route that peer Directly.
//      Pairs still unconfirmed when the punch window closes are told to route
//      via server Relay (unless the match is Forced-P2P).
//   4. RelayData packets are forwarded peer→peer (TURN fallback). KeepAlives
//      refresh liveness and NAT bindings.
//
// Everything is additive and inert unless a match opts in (P2PMode != Off), so
// classic dedicated-server matches are entirely unaffected.

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using Microsoft.Extensions.Logging;

namespace OVS.Rollback.P2P
{
    /// <summary>Per-match P2P parameters resolved from the OVS match config.</summary>
    public sealed record P2PMatchInfo(
        P2PMode Mode,
        int ExpectedPeers,
        IReadOnlyDictionary<ushort, bool> HostByIndex);

    /// <summary>Tunable coordination parameters (mirrors ServerConfiguration.Networking.P2P).</summary>
    public sealed record P2PSettings(
        bool Enabled,
        int RegistrationTimeoutMs,
        int PunchWindowMs,
        byte PunchAttempts,
        ushort PunchIntervalMs,
        ushort PunchStartDelayMs,
        int PeerLivenessTimeoutMs,
        bool RelayEnabled)
    {
        public static P2PSettings Defaults => new(
            Enabled: false,
            RegistrationTimeoutMs: 4000,
            PunchWindowMs: 3000,
            PunchAttempts: 8,
            PunchIntervalMs: 100,
            PunchStartDelayMs: 250,
            PeerLivenessTimeoutMs: 15000,
            RelayEnabled: true);
    }

    public sealed class P2PCoordinator : IDisposable
    {
        private sealed class Session
        {
            public required string MatchId { get; init; }
            public required string Key { get; init; }
            public P2PMode Mode { get; set; }
            public int ExpectedPeers { get; set; }
            public IReadOnlyDictionary<ushort, bool> HostByIndex { get; set; } = new Dictionary<ushort, bool>();
            public readonly ConcurrentDictionary<ushort, P2PPeer> Peers = new();
            public P2PSessionPhase Phase = P2PSessionPhase.Registering;
            public long CreatedTs;
            public long PunchStartedTs;
            // Bumped on every inbound control packet for this session. The
            // eviction sweep captures it and only removes if it hasn't changed,
            // closing the "evict a peer that just refreshed" race.
            public long LastActivityTs;
            public ushort HostIndex;
            public bool HostResolved;
            public readonly object Gate = new();
            // Pairs already told their final route, so we don't spam UseDirect/UseRelay.
            public readonly ConcurrentDictionary<(ushort, ushort), PeerRoute> ResolvedPairs = new();
        }

        private readonly ConcurrentDictionary<string, Session> _sessions = new();
        // Per-match config, populated by EnableMatch (off the receive path).
        private readonly ConcurrentDictionary<string, P2PMatchInfo> _matchInfo = new();
        // Registrations that arrived before EnableMatch; drained when it fires.
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<ushort, IPEndPoint>> _pending = new();
        // Maps a live reflexive endpoint back to (matchId, playerIndex) so packets
        // that don't carry a matchId (PunchResult/KeepAlive/RelayData) can be routed.
        private readonly ConcurrentDictionary<string, (string matchId, ushort index)> _endpointOwners = new();

        private readonly Action<byte[], IPEndPoint> _send;
        private readonly ILogger _logger;
        private readonly P2PSettings _settings;
        private readonly Timer _sweepTimer;
        private volatile bool _disposed;

        public P2PCoordinator(
            Action<byte[], IPEndPoint> send,
            P2PSettings settings,
            ILogger logger)
        {
            _send = send;
            _settings = settings;
            _logger = logger;
            // Sweep drives the registration/punch timeouts and liveness eviction.
            _sweepTimer = new Timer(_ => SafeSweep(), null, 500, 500);
        }

        // ═══════════════════════════════════════════
        //  Match enablement (called off the receive path)
        // ═══════════════════════════════════════════

        /// <summary>
        /// Register a match's P2P parameters. Called by the rollback server when
        /// it creates the match (config already fetched), never from the UDP
        /// receive loop. Idempotent. Drains any registrations that raced ahead.
        /// </summary>
        public void EnableMatch(string matchId, P2PMatchInfo info)
        {
            if (!_settings.Enabled || info.Mode == P2PMode.Off) return;

            _matchInfo[matchId] = info;

            // Drain early registrations that arrived before this call.
            if (_pending.TryRemove(matchId, out var early))
            {
                foreach (var kv in early)
                    IngestRegistration(matchId, kv.Key, kv.Value);
            }
        }

        // ═══════════════════════════════════════════
        //  Inbound dispatch
        // ═══════════════════════════════════════════

        public void HandleControl(in P2PInbound msg, IPEndPoint remote)
        {
            if (!_settings.Enabled) return;

            switch (msg.Subtype)
            {
                case P2PSubtype.Register:     HandleRegister(msg, remote); break;
                case P2PSubtype.PunchResult:  HandlePunchResult(msg, remote); break;
                case P2PSubtype.RelayData:    HandleRelay(msg, remote); break;
                case P2PSubtype.KeepAlive:    HandleKeepAlive(msg, remote); break;
                default: break; // server→client subtypes are not expected inbound
            }
        }

        private void HandleRegister(in P2PInbound msg, IPEndPoint remote)
        {
            string matchId = msg.MatchId;
            ushort playerIndex = msg.PlayerIndex;
            var endpoint = new IPEndPoint(remote.Address, remote.Port);

            // If the match hasn't been enabled yet (Register raced ahead of match
            // creation), buffer the endpoint and return. NO config fetch happens
            // here — the receive loop must never block on the network.
            if (!_matchInfo.ContainsKey(matchId))
            {
                var bucket = _pending.GetOrAdd(matchId, _ => new ConcurrentDictionary<ushort, IPEndPoint>());
                bucket[playerIndex] = endpoint;
                return;
            }

            IngestRegistration(matchId, playerIndex, endpoint);
        }

        private void IngestRegistration(string matchId, ushort playerIndex, IPEndPoint endpoint)
        {
            if (!_matchInfo.TryGetValue(matchId, out var info) || info.Mode == P2PMode.Off)
                return;

            var session = _sessions.GetOrAdd(matchId, id => new Session
            {
                MatchId = id,
                Key = string.Empty,
                Mode = info.Mode,
                ExpectedPeers = info.ExpectedPeers,
                HostByIndex = info.HostByIndex,
                CreatedTs = Stopwatch.GetTimestamp(),
                LastActivityTs = Stopwatch.GetTimestamp(),
            });

            long now = Stopwatch.GetTimestamp();
            session.LastActivityTs = now;

            IPEndPoint? oldEp = null;
            var peer = session.Peers.AddOrUpdate(playerIndex,
                _ => new P2PPeer(playerIndex, endpoint, now),
                (_, existing) =>
                {
                    if (!existing.ReflexiveEndpoint.Equals(endpoint))
                        oldEp = existing.ReflexiveEndpoint;
                    existing.ReflexiveEndpoint = endpoint;
                    existing.LastSeenTimestamp = now;
                    return existing;
                });

            // Retire the previous endpoint mapping so _endpointOwners can't leak
            // stale keys (and can't misroute a later, reused address) when a peer
            // re-registers from a rebound NAT port.
            if (oldEp is not null)
                RemoveOwnerIf(oldEp, matchId, playerIndex);
            _endpointOwners[EndpointKey(endpoint)] = (matchId, playerIndex);

            var role = ResolveRole(session, playerIndex);
            peer.Role = role;
            _send(P2PControl.BuildRegisterAck(role, session.Mode, endpoint), endpoint);

            _logger.LogInformation(
                "[P2P] Register match={Match} idx={Idx} reflexive={Ep} role={Role} ({Have}/{Want})",
                matchId, playerIndex, endpoint, role, session.Peers.Count, session.ExpectedPeers);

            TryStartPunching(session);
        }

        private void HandlePunchResult(in P2PInbound msg, IPEndPoint remote)
        {
            if (!_endpointOwners.TryGetValue(EndpointKey(remote), out var owner)) return;
            if (!_sessions.TryGetValue(owner.matchId, out var session)) return;
            if (!session.Peers.TryGetValue(owner.index, out var peer)) return;

            long now = Stopwatch.GetTimestamp();
            peer.LastSeenTimestamp = now;
            session.LastActivityTs = now;
            peer.PunchConfirmed[msg.PeerIndex] = msg.Success;

            if (msg.Success)
            {
                _logger.LogDebug("[P2P] PunchResult match={Match} {A}->{B} OK", owner.matchId, owner.index, msg.PeerIndex);
                TryResolvePair(session, owner.index, msg.PeerIndex);
            }
        }

        private void HandleRelay(in P2PInbound msg, IPEndPoint remote)
        {
            if (!_settings.RelayEnabled) return;
            if (!_endpointOwners.TryGetValue(EndpointKey(remote), out var owner)) return;
            if (!_sessions.TryGetValue(owner.matchId, out var session)) return;

            // Forced-P2P means direct-only: the server must not act as a TURN
            // relay even if a client asks it to. This mirrors FailPairToRelay's
            // control-path guard so Forced is enforced on the data path too.
            if (session.Mode == P2PMode.Forced) return;

            if (!session.Peers.TryGetValue(msg.DstPlayerIndex, out var dst)) return;

            session.LastActivityTs = Stopwatch.GetTimestamp();

            // Forward opaque (already game-compressed) bytes to the destination
            // peer, tagged with the (authenticated, endpoint-derived) source index.
            var packet = P2PControl.BuildRelayDeliver(owner.index, msg.Data.Span);
            _send(packet, dst.ReflexiveEndpoint);
        }

        private void HandleKeepAlive(in P2PInbound msg, IPEndPoint remote)
        {
            if (!_endpointOwners.TryGetValue(EndpointKey(remote), out var owner))
            {
                // Endpoint rebind we haven't been told about via Register. We
                // can't safely re-associate without a matchId, so wait for a
                // fresh Register (the client contract retransmits it).
                return;
            }
            if (!_sessions.TryGetValue(owner.matchId, out var session)) return;
            if (session.Peers.TryGetValue(owner.index, out var peer))
            {
                long now = Stopwatch.GetTimestamp();
                peer.LastSeenTimestamp = now;
                session.LastActivityTs = now;
                if (!peer.ReflexiveEndpoint.Equals(remote))
                {
                    var oldEp = peer.ReflexiveEndpoint;
                    peer.ReflexiveEndpoint = new IPEndPoint(remote.Address, remote.Port);
                    RemoveOwnerIf(oldEp, owner.matchId, owner.index);
                    _endpointOwners[EndpointKey(remote)] = owner;
                }
            }
        }

        // ═══════════════════════════════════════════
        //  Coordination logic
        // ═══════════════════════════════════════════

        private void TryStartPunching(Session session)
        {
            lock (session.Gate)
            {
                if (session.Phase != P2PSessionPhase.Registering) return;
                if (session.Peers.Count < session.ExpectedPeers) return;
                BeginPunchPhase(session);
            }
        }

        // Caller must hold session.Gate.
        private void BeginPunchPhase(Session session)
        {
            session.Phase = P2PSessionPhase.Punching;
            session.PunchStartedTs = Stopwatch.GetTimestamp();
            ResolveHost(session);

            var peerList = session.Peers.Values
                .OrderBy(p => p.PlayerIndex)
                .Select(p => (p.PlayerIndex, p.Role, p.ReflexiveEndpoint))
                .ToList();

            var listPacket = P2PControl.BuildPeerList(peerList);
            var punchPacket = P2PControl.BuildPunchNow(
                _settings.PunchStartDelayMs, _settings.PunchAttempts, _settings.PunchIntervalMs);

            foreach (var peer in session.Peers.Values)
            {
                _send(listPacket, peer.ReflexiveEndpoint);
                _send(punchPacket, peer.ReflexiveEndpoint);
            }

            _logger.LogInformation(
                "[P2P] Punch phase match={Match} peers={Count} host={Host} attempts={Att} interval={Int}ms",
                session.MatchId, session.Peers.Count, session.HostIndex,
                _settings.PunchAttempts, _settings.PunchIntervalMs);
        }

        // Caller must hold session.Gate.
        private void ResolveHost(Session session)
        {
            if (session.HostResolved) return;

            // 1) Honour an explicit host flag from the backend config.
            foreach (var kv in session.HostByIndex)
            {
                if (kv.Value && session.Peers.ContainsKey(kv.Key))
                {
                    session.HostIndex = kv.Key;
                    session.HostResolved = true;
                    ApplyRoles(session);
                    return;
                }
            }

            // 2) Fallback: lowest registered PlayerIndex hosts. Deterministic and
            //    stable across the session so every peer agrees on the authority.
            session.HostIndex = session.Peers.Keys.Min();
            session.HostResolved = true;
            ApplyRoles(session);
        }

        private void ApplyRoles(Session session)
        {
            foreach (var peer in session.Peers.Values)
                peer.Role = peer.PlayerIndex == session.HostIndex ? PeerRole.Host : PeerRole.Guest;
        }

        private PeerRole ResolveRole(Session session, ushort index)
        {
            if (session.HostResolved)
                return index == session.HostIndex ? PeerRole.Host : PeerRole.Guest;
            // Pre-election: reflect the config hint if present, else Guest for now.
            return session.HostByIndex.TryGetValue(index, out bool h) && h ? PeerRole.Host : PeerRole.Guest;
        }

        private void TryResolvePair(Session session, ushort a, ushort b)
        {
            var pairKey = PairKey(a, b);
            if (session.ResolvedPairs.ContainsKey(pairKey)) return;

            if (!session.Peers.TryGetValue(a, out var pa) || !session.Peers.TryGetValue(b, out var pb))
                return;

            bool aToB = pa.PunchConfirmed.TryGetValue(b, out bool ab) && ab;
            bool bToA = pb.PunchConfirmed.TryGetValue(a, out bool ba) && ba;

            // A NAT hole is only usable when both directions are open.
            if (aToB && bToA)
            {
                if (session.ResolvedPairs.TryAdd(pairKey, PeerRoute.Direct))
                {
                    _send(P2PControl.BuildUseDirect(b, pb.ReflexiveEndpoint), pa.ReflexiveEndpoint);
                    _send(P2PControl.BuildUseDirect(a, pa.ReflexiveEndpoint), pb.ReflexiveEndpoint);
                    _logger.LogInformation("[P2P] Direct route match={Match} {A}<->{B}", session.MatchId, a, b);
                    MaybeEstablished(session);
                }
            }
        }

        private void FailPairToRelay(Session session, ushort a, ushort b)
        {
            var pairKey = PairKey(a, b);
            if (!session.ResolvedPairs.TryAdd(pairKey, PeerRoute.Relay)) return;

            if (session.Mode == P2PMode.Forced)
            {
                // Forced-P2P: no relay. Nothing to tell the client — it stays on
                // the dedicated authority path, which is the safe default anyway.
                _logger.LogWarning("[P2P] Pair failed and mode=Forced match={Match} {A}<->{B} (no relay)",
                    session.MatchId, a, b);
                return;
            }

            if (!_settings.RelayEnabled) return;
            if (!session.Peers.TryGetValue(a, out var pa) || !session.Peers.TryGetValue(b, out var pb))
                return;

            _send(P2PControl.BuildUseRelay(b), pa.ReflexiveEndpoint);
            _send(P2PControl.BuildUseRelay(a), pb.ReflexiveEndpoint);
            _logger.LogInformation("[P2P] Relay route match={Match} {A}<->{B}", session.MatchId, a, b);
            MaybeEstablished(session);
        }

        private void MaybeEstablished(Session session)
        {
            int want = session.Peers.Count * (session.Peers.Count - 1) / 2;
            if (session.ResolvedPairs.Count >= want && want > 0)
            {
                if (session.Phase != P2PSessionPhase.Established)
                {
                    session.Phase = P2PSessionPhase.Established;
                    _logger.LogInformation("[P2P] All {Count} pair(s) resolved for match={Match}",
                        want, session.MatchId);
                }
            }
        }

        // ═══════════════════════════════════════════
        //  Timeout / liveness sweep
        // ═══════════════════════════════════════════

        private void SafeSweep()
        {
            if (_disposed) return;
            try { Sweep(); }
            catch (Exception ex) { _logger.LogError(ex, "[P2P] sweep error"); }
        }

        private void Sweep()
        {
            foreach (var session in _sessions.Values)
            {
                lock (session.Gate)
                {
                    // Registration stalled: proceed with whoever is present (>=2) or give up.
                    if (session.Phase == P2PSessionPhase.Registering)
                    {
                        double waited = Stopwatch.GetElapsedTime(session.CreatedTs).TotalMilliseconds;
                        if (waited >= _settings.RegistrationTimeoutMs)
                        {
                            if (session.Peers.Count >= 2)
                            {
                                _logger.LogWarning(
                                    "[P2P] Registration timeout match={Match}, proceeding with {Count}/{Want}",
                                    session.MatchId, session.Peers.Count, session.ExpectedPeers);
                                BeginPunchPhase(session);
                            }
                            else
                            {
                                session.Phase = P2PSessionPhase.FellBack;
                                _logger.LogWarning(
                                    "[P2P] Registration failed match={Match} ({Count} peer). Falling back to authority.",
                                    session.MatchId, session.Peers.Count);
                            }
                        }
                    }

                    // Punch window closed: any pair still Pending → relay (or fall back).
                    if (session.Phase == P2PSessionPhase.Punching)
                    {
                        double punching = Stopwatch.GetElapsedTime(session.PunchStartedTs).TotalMilliseconds;
                        if (punching >= _settings.PunchWindowMs)
                        {
                            var indices = session.Peers.Keys.OrderBy(x => x).ToArray();
                            for (int i = 0; i < indices.Length; i++)
                                for (int j = i + 1; j < indices.Length; j++)
                                {
                                    if (!session.ResolvedPairs.ContainsKey(PairKey(indices[i], indices[j])))
                                        FailPairToRelay(session, indices[i], indices[j]);
                                }
                        }
                    }
                }
            }

            // Evict dead sessions (all peers gone silent). Re-checked under the
            // gate against LastActivityTs, which every inbound handler bumps, so
            // a session that just saw a Register/KeepAlive is never torn down.
            foreach (var kv in _sessions)
            {
                var session = kv.Value;
                lock (session.Gate)
                {
                    if (session.Peers.Count == 0) continue;
                    double idle = Stopwatch.GetElapsedTime(session.LastActivityTs).TotalMilliseconds;
                    if (idle < _settings.PeerLivenessTimeoutMs) continue;

                    if (_sessions.TryRemove(kv.Key, out _))
                    {
                        PurgeOwnersForMatch(kv.Key);
                        _matchInfo.TryRemove(kv.Key, out _);
                        _pending.TryRemove(kv.Key, out _);
                        _logger.LogInformation("[P2P] Evicted idle session match={Match}", session.MatchId);
                    }
                }
            }
        }

        /// <summary>Drop a match's coordination state (called when the authority match ends).</summary>
        public void EndMatch(string matchId)
        {
            _sessions.TryRemove(matchId, out _);
            _matchInfo.TryRemove(matchId, out _);
            _pending.TryRemove(matchId, out _);
            PurgeOwnersForMatch(matchId);
        }

        // ═══════════════════════════════════════════
        //  Helpers
        // ═══════════════════════════════════════════

        private static string EndpointKey(IPEndPoint ep) => $"{ep.Address}:{ep.Port}";
        private static (ushort, ushort) PairKey(ushort a, ushort b) => a < b ? (a, b) : (b, a);

        /// <summary>Remove an endpoint→owner mapping only if it still points at the given owner.</summary>
        private void RemoveOwnerIf(IPEndPoint ep, string matchId, ushort index)
        {
            var key = EndpointKey(ep);
            if (_endpointOwners.TryGetValue(key, out var cur) && cur.matchId == matchId && cur.index == index)
            {
                // Guard against racing a legitimate re-use: only remove the exact pair.
                ((ICollection<KeyValuePair<string, (string matchId, ushort index)>>)_endpointOwners)
                    .Remove(new KeyValuePair<string, (string, ushort)>(key, cur));
            }
        }

        /// <summary>Purge every endpoint mapping belonging to a match (used on eviction/EndMatch).</summary>
        private void PurgeOwnersForMatch(string matchId)
        {
            foreach (var kv in _endpointOwners)
            {
                if (kv.Value.matchId == matchId)
                {
                    ((ICollection<KeyValuePair<string, (string matchId, ushort index)>>)_endpointOwners)
                        .Remove(kv);
                }
            }
        }

        public void Dispose()
        {
            _disposed = true;
            _sweepTimer.Dispose();
            _sessions.Clear();
            _endpointOwners.Clear();
            _matchInfo.Clear();
            _pending.Clear();
        }
    }
}
