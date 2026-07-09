// P2PClient.cs
//
// The client half of the hole-punching handshake, run by a rollback exe when it
// operates as a P2P host or guest. It is the mirror of P2PCoordinator:
//
//   1. Register with the coordinator (retransmit until RegisterAck), learning
//      our own public/reflexive endpoint + assigned role.
//   2. On PeerList, record every peer's reflexive endpoint.
//   3. On PunchNow, fire magic-framed Punch probes at each peer's endpoint
//      (opens our NAT); when a Punch arrives FROM a peer, report PunchResult
//      success for that peer.
//   4. On UseDirect, mark that peer routed Direct at the given endpoint; on
//      UseRelay, mark it routed via the coordinator's relay.
//   5. Send periodic KeepAlives so the NAT mappings stay open.
//
// It is socket-agnostic: outbound goes through a send delegate and inbound is
// fed via HandleDatagram(). The exe wires those to its UDP socket; a test can
// wire them to loopback sockets. HandleDatagram returns true when it consumed a
// P2P control/punch packet, false when the datagram is game traffic the exe
// should process normally.

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using Microsoft.Extensions.Logging;

namespace OVS.Rollback.P2P
{
    public sealed class P2PClient : IDisposable
    {
        public enum Route { Pending, Direct, Relay }

        private readonly Action<byte[], IPEndPoint> _send;
        private readonly IPEndPoint _coordinator;
        private readonly string _matchId;
        private readonly string _key;
        private readonly ushort _myIndex;
        private readonly P2PSettings _settings;
        private readonly ILogger _logger;

        private readonly ConcurrentDictionary<ushort, IPEndPoint> _peers = new();        // index → reflexive endpoint
        private readonly ConcurrentDictionary<ushort, Route> _routes = new();             // index → resolved route
        private readonly ConcurrentDictionary<ushort, IPEndPoint> _directEndpoints = new(); // index → direct endpoint (UseDirect)
        private readonly ConcurrentDictionary<ushort, bool> _inboundPunchSeen = new();    // peers we've received a Punch from

        private readonly object _gate = new();
        private readonly Timer _timer;
        private volatile bool _registered;
        private volatile bool _disposed;

        // Punch scheduling (set by PunchNow).
        private long _nextPunchTs;
        private int _punchesLeft;
        private ushort _punchIntervalMs;

        public PeerRole Role { get; private set; } = PeerRole.Guest;
        public IPEndPoint? PublicEndpoint { get; private set; }
        public bool IsRegistered => _registered;

        public P2PClient(
            Action<byte[], IPEndPoint> send,
            IPEndPoint coordinator,
            string matchId,
            string key,
            ushort myIndex,
            P2PSettings settings,
            ILogger logger)
        {
            _send = send;
            _coordinator = coordinator;
            _matchId = matchId;
            _key = key;
            _myIndex = myIndex;
            _settings = settings;
            _logger = logger;
            _timer = new Timer(_ => Tick(), null, Timeout.Infinite, Timeout.Infinite);
        }

        public void Start()
        {
            SendRegister();
            // ~250ms cadence drives register retransmit, the punch burst, and keepalives.
            _timer.Change(250, 250);
        }

        /// <summary>The resolved route for a peer (Pending until the coordinator decides).</summary>
        public Route RouteFor(ushort peerIndex) => _routes.TryGetValue(peerIndex, out var r) ? r : Route.Pending;

        /// <summary>The direct endpoint for a peer, if the coordinator resolved it Direct.</summary>
        public IPEndPoint? DirectEndpointFor(ushort peerIndex) => _directEndpoints.TryGetValue(peerIndex, out var ep) ? ep : null;

        /// <summary>Endpoint of the elected host (for a guest to forward its game to). Null until known.</summary>
        public IPEndPoint? HostEndpoint()
        {
            foreach (var kv in _peers)
                if (kv.Key != _myIndex && _routes.TryGetValue(kv.Key, out var r) && r == Route.Direct
                    && _directEndpoints.TryGetValue(kv.Key, out var ep))
                    return ep;
            return null;
        }

        // ═══════════════════════════════════════════
        //  Inbound
        // ═══════════════════════════════════════════

        /// <summary>Feed every received datagram. Returns true if it was a P2P control/punch packet (consumed).</summary>
        public bool HandleDatagram(ReadOnlySpan<byte> data, IPEndPoint from)
        {
            if (!P2PControl.IsControlPacket(data)) return false;

            byte subtype = data[5];
            if (subtype == (byte)P2PSubtype.Punch)
            {
                // Peer → us: a hole is open in the incoming direction. Confirm it.
                var parsed = P2PControl.Parse(data);
                if (parsed.HasValue)
                {
                    ushort peer = parsed.Value.PlayerIndex;
                    if (_inboundPunchSeen.TryAdd(peer, true))
                        _logger.LogDebug("[P2PClient {Idx}] inbound punch from peer {Peer}", _myIndex, peer);
                    // Report success to the coordinator (idempotent; it dedups).
                    _send(P2PControl.BuildPunchResult(_myIndex, peer, true), _coordinator);
                }
                return true;
            }

            // Otherwise it's a server→client control message from the coordinator.
            var srv = P2PControl.ParseServer(data);
            if (srv.HasValue) HandleServer(srv.Value);
            return true;
        }

        private void HandleServer(in P2PControl.P2PServerInbound msg)
        {
            switch (msg.Subtype)
            {
                case P2PSubtype.RegisterAck:
                    _registered = true;
                    Role = msg.Role;
                    PublicEndpoint = msg.Endpoint;
                    _logger.LogInformation("[P2PClient {Idx}] registered; role={Role} public={Ep}",
                        _myIndex, Role, msg.Endpoint);
                    break;

                case P2PSubtype.PeerList:
                    if (msg.Peers is not null)
                    {
                        foreach (var (index, role, ep) in msg.Peers)
                        {
                            if (index == _myIndex) { Role = role; continue; }
                            _peers[index] = ep;
                            _routes.TryAdd(index, Route.Pending);
                        }
                        _logger.LogInformation("[P2PClient {Idx}] peer list: {Count} peer(s)", _myIndex, _peers.Count);
                    }
                    break;

                case P2PSubtype.PunchNow:
                    lock (_gate)
                    {
                        _punchesLeft = Math.Max(1, (int)msg.Attempts);
                        _punchIntervalMs = msg.IntervalMs == 0 ? (ushort)100 : msg.IntervalMs;
                        _nextPunchTs = Stopwatch.GetTimestamp() +
                            (long)(msg.StartDelayMs / 1000.0 * Stopwatch.Frequency);
                    }
                    _logger.LogInformation("[P2PClient {Idx}] punch scheduled: {N} × {Int}ms",
                        _myIndex, _punchesLeft, _punchIntervalMs);
                    break;

                case P2PSubtype.UseDirect:
                    if (msg.Endpoint is not null)
                    {
                        _directEndpoints[msg.PeerIndex] = msg.Endpoint;
                        _routes[msg.PeerIndex] = Route.Direct;
                        _logger.LogInformation("[P2PClient {Idx}] peer {Peer} → DIRECT {Ep}",
                            _myIndex, msg.PeerIndex, msg.Endpoint);
                    }
                    break;

                case P2PSubtype.UseRelay:
                    _routes[msg.PeerIndex] = Route.Relay;
                    _logger.LogInformation("[P2PClient {Idx}] peer {Peer} → RELAY", _myIndex, msg.PeerIndex);
                    break;
            }
        }

        // ═══════════════════════════════════════════
        //  Timer-driven output
        // ═══════════════════════════════════════════

        private void Tick()
        {
            if (_disposed) return;
            try
            {
                if (!_registered)
                {
                    SendRegister(); // retransmit until acked
                    return;
                }

                // Fire punch bursts at each peer when scheduled.
                bool doPunch;
                lock (_gate)
                {
                    doPunch = _punchesLeft > 0 && Stopwatch.GetTimestamp() >= _nextPunchTs;
                    if (doPunch)
                    {
                        _punchesLeft--;
                        _nextPunchTs = Stopwatch.GetTimestamp() +
                            (long)(_punchIntervalMs / 1000.0 * Stopwatch.Frequency);
                    }
                }
                if (doPunch)
                {
                    var punch = P2PControl.BuildPunch(_myIndex);
                    foreach (var kv in _peers)
                        _send(punch, kv.Value);
                }

                // Keepalive to the coordinator so our reflexive mapping stays put.
                _send(P2PControl.BuildKeepAlive(_myIndex), _coordinator);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[P2PClient {Idx}] tick error", _myIndex);
            }
        }

        private void SendRegister() => _send(P2PControl.BuildRegister(_myIndex, _matchId, _key), _coordinator);

        public void Dispose()
        {
            _disposed = true;
            _timer.Dispose();
        }
    }
}
