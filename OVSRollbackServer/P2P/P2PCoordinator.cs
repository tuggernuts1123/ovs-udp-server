// P2PCoordinator.cs
//
// The server-side brain of the hole-punching layer. One coordinator instance
// lives on the rollback server and manages a coordination session per match.
//
// Flow (see docs/P2P_HOLE_PUNCHING.md for the full picture):
//   1. Each match player's ASI sends a Register control packet. The server
//      records the reflexive (public) endpoint it observed and, on the first
//      registration for a match, looks up the match's P2P config.
//   2. Once every expected participant has registered (or a short timeout
//      elapses with at least two present), the coordinator elects a host,
//      broadcasts the full PeerList, and issues a synchronized PunchNow so all
//      clients fire UDP punches at each other at the same moment.
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
            public ushort HostIndex;
            public bool HostResolved;
            public readonly object Gate = new();
            // Pairs already told their final route, so we don't spam UseDirect/UseRelay.
            public readonly ConcurrentDictionary<(ushort, ushort), PeerRoute> ResolvedPairs = new();
        }

        private readonly ConcurrentDictionary<string, Session> _sessions = new();
        // Maps a live reflexive endpoint back to (matchId, playerIndex) so packets
        // that don't carry a matchId (PunchResult/KeepAlive/RelayData) can be routed.
        private readonly ConcurrentDictionary<string, (string matchId, ushort index)> _endpointOwners = new();

        private readonly Action<byte[], IPEndPoint> _send;
        private readonly Func<string, string, P2PMatchInfo?> _matchInfoProvider;
        private readonly ILogger _logger;
        private readonly P2PSettings _settings;
        private readonly Timer _sweepTimer;
        private volatile bool _disposed;

        public P2PCoordinator(
            Action<byte[], IPEndPoint> send,
            Func<string, string, P2PMatchInfo?> matchInfoProvider,
            P2PSettings settings,
            ILogger logger)
        {
            _send = send;
            _matchInfoProvider = matchInfoProvider;
            _settings = settings;
            _logger = logger;
            // Sweep drives the registration/punch timeouts and liveness eviction.
            _sweepTimer = new Timer(_ => SafeSweep(), null, 500, 500);
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
            // Copy out of the 'in' struct so the values can be captured by the
            // concurrent-dictionary lambdas below (CS1628 otherwise).
            string matchId = msg.MatchId;
            string key = msg.Key;
            ushort playerIndex = msg.PlayerIndex;

            var info = _matchInfoProvider(matchId, key);
            if (info is null || info.Mode == P2PMode.Off)
            {
                // Backend didn't enable P2P for this match; ignore silently so
                // an over-eager client can't spin up coordination state.
                return;
            }

            var session = _sessions.GetOrAdd(matchId, id => new Session
            {
                MatchId = id,
                Key = key,
                Mode = info.Mode,
                ExpectedPeers = info.ExpectedPeers,
                HostByIndex = info.HostByIndex,
                CreatedTs = Stopwatch.GetTimestamp(),
            });

            long now = Stopwatch.GetTimestamp();
            var endpoint = new IPEndPoint(remote.Address, remote.Port);

            var peer = session.Peers.AddOrUpdate(playerIndex,
                _ => new P2PPeer(playerIndex, endpoint, now),
                (_, existing) =>
                {
                    existing.ReflexiveEndpoint = endpoint;
                    existing.LastSeenTimestamp = now;
                    return existing;
                });

            _endpointOwners[EndpointKey(endpoint)] = (matchId, playerIndex);

            // Immediately echo the reflexive endpoint so the client learns its
            // own public IP:port (STUN behaviour) and its assigned role.
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

            peer.LastSeenTimestamp = Stopwatch.GetTimestamp();
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
            if (!session.Peers.TryGetValue(msg.DstPlayerIndex, out var dst)) return;

            // Forward opaque (already game-compressed) bytes to the destination
            // peer, tagged with the source index so the receiver can attribute it.
            var packet = P2PControl.BuildRelayDeliver(owner.index, msg.Data.Span);
            _send(packet, dst.ReflexiveEndpoint);
        }

        private void HandleKeepAlive(in P2PInbound msg, IPEndPoint remote)
        {
            if (!_endpointOwners.TryGetValue(EndpointKey(remote), out var owner))
            {
                // Endpoint rebind (NAT changed the mapping). We can't safely
                // re-associate without a matchId, so wait for a fresh Register.
                return;
            }
            if (!_sessions.TryGetValue(owner.matchId, out var session)) return;
            if (session.Peers.TryGetValue(owner.index, out var peer))
            {
                peer.LastSeenTimestamp = Stopwatch.GetTimestamp();
                if (!peer.ReflexiveEndpoint.Equals(remote))
                {
                    peer.ReflexiveEndpoint = new IPEndPoint(remote.Address, remote.Port);
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
            long now = Stopwatch.GetTimestamp();

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

            // Evict dead sessions (all peers gone silent). Coordination state is
            // cheap but we don't want it to accumulate across a long-lived server.
            foreach (var kv in _sessions)
            {
                var session = kv.Value;
                bool anyAlive = false;
                foreach (var peer in session.Peers.Values)
                {
                    double idle = Stopwatch.GetElapsedTime(peer.LastSeenTimestamp).TotalMilliseconds;
                    if (idle < _settings.PeerLivenessTimeoutMs) { anyAlive = true; break; }
                }
                if (!anyAlive && session.Peers.Count > 0)
                {
                    if (_sessions.TryRemove(kv.Key, out _))
                    {
                        foreach (var peer in session.Peers.Values)
                            _endpointOwners.TryRemove(EndpointKey(peer.ReflexiveEndpoint), out _);
                        _logger.LogInformation("[P2P] Evicted idle session match={Match}", session.MatchId);
                    }
                }
            }
        }

        /// <summary>Drop a match's coordination state (called when the authority match ends).</summary>
        public void EndMatch(string matchId)
        {
            if (_sessions.TryRemove(matchId, out var session))
            {
                foreach (var peer in session.Peers.Values)
                    _endpointOwners.TryRemove(EndpointKey(peer.ReflexiveEndpoint), out _);
            }
        }

        // ═══════════════════════════════════════════
        //  Helpers
        // ═══════════════════════════════════════════

        private static string EndpointKey(IPEndPoint ep) => $"{ep.Address}:{ep.Port}";
        private static (ushort, ushort) PairKey(ushort a, ushort b) => a < b ? (a, b) : (b, a);

        public void Dispose()
        {
            _disposed = true;
            _sweepTimer.Dispose();
            _sessions.Clear();
            _endpointOwners.Clear();
        }
    }
}
