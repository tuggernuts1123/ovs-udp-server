// P2PModels.cs
//
// Data models for the OVS peer-to-peer hole-punching layer.
//
// The rollback server historically operates as a dedicated, frame-authoritative
// relay: every player's input travels client -> cloud server -> every other
// client. For two players who are geographically close to each other but far
// from the cloud region, that relay hop roughly doubles the latency they feel
// versus talking directly.
//
// This module lets the cloud server act as a rendezvous / coordination point
// (STUN-like) that helps peers open direct UDP paths through their home NATs,
// with the server remaining available as a relay (TURN-like) when a direct
// path can't be established (e.g. symmetric NAT). The authoritative rollback
// role still lives on one endpoint (the "host"); see docs/P2P_HOLE_PUNCHING.md.
//
// Everything here is inert unless a match's config opts in via P2PMode, so it
// cannot affect existing dedicated-server matches.

using System.Net;

namespace OVS.Rollback.P2P
{
    /// <summary>
    /// Per-match P2P behaviour, supplied by the OVS backend in the match config.
    /// </summary>
    public enum P2PMode : byte
    {
        /// <summary>No P2P. Classic dedicated-server relay (default, unchanged).</summary>
        Off = 0,

        /// <summary>Try to establish direct peer paths; fall back to relay/authority if punching fails.</summary>
        Preferred = 1,

        /// <summary>Direct peer paths only; do not fall back to server relay (diagnostics / LAN).</summary>
        Forced = 2,
    }

    /// <summary>Role assigned to a peer within a P2P match.</summary>
    public enum PeerRole : byte
    {
        Guest = 0,
        Host = 1,
    }

    /// <summary>How a given peer pair is currently routing traffic.</summary>
    public enum PeerRoute : byte
    {
        /// <summary>Not decided yet — punching in progress.</summary>
        Pending = 0,

        /// <summary>Direct hole succeeded; peers exchange traffic directly.</summary>
        Direct = 1,

        /// <summary>Punch failed; traffic is relayed through the cloud server.</summary>
        Relay = 2,
    }

    /// <summary>
    /// One participant in a P2P coordination session. Tracks the reflexive
    /// (server-observed) endpoint and the punch outcome against every peer.
    /// </summary>
    public sealed class P2PPeer
    {
        public ushort PlayerIndex { get; init; }
        public PeerRole Role { get; set; } = PeerRole.Guest;

        /// <summary>Public endpoint as observed by the server on the last control packet.</summary>
        public IPEndPoint ReflexiveEndpoint { get; set; }

        /// <summary>Monotonic timestamp (Stopwatch ticks) of the last control packet from this peer.</summary>
        public long LastSeenTimestamp { get; set; }

        /// <summary>Whether this peer has confirmed a direct hole to peer[index]. Keyed by peer PlayerIndex.</summary>
        public System.Collections.Concurrent.ConcurrentDictionary<ushort, bool> PunchConfirmed { get; } = new();

        public P2PPeer(ushort playerIndex, IPEndPoint reflexive, long ts)
        {
            PlayerIndex = playerIndex;
            ReflexiveEndpoint = reflexive;
            LastSeenTimestamp = ts;
        }
    }

    /// <summary>
    /// Lifecycle of a match's coordination session.
    /// </summary>
    public enum P2PSessionPhase : byte
    {
        /// <summary>Waiting for peers to register on the P2P control channel.</summary>
        Registering = 0,

        /// <summary>PunchNow issued; waiting for PunchResult reports.</summary>
        Punching = 1,

        /// <summary>All pairs resolved (direct or relay). Steady state.</summary>
        Established = 2,

        /// <summary>Coordination gave up; the match falls back to cloud authority.</summary>
        FellBack = 3,
    }
}
