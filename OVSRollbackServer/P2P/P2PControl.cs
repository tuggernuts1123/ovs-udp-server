// P2PControl.cs
//
// Wire format for the OVS P2P control channel.
//
// P2P control packets share the same UDP socket as game rollback traffic, so
// they MUST be unambiguously distinguishable from a game packet. Game packets
// are bitmask-compressed (CompressionHelper) and their first byte is a
// compression mask, so we cannot key off a message-type byte the way the game
// protocol does.
//
// Instead every control packet is framed with a fixed 6-byte header:
//
//   [0..3]  Magic  = 'O' 'V' 'S' 'P'  (0x4F 0x56 0x53 0x50)
//   [4]     Version                    (currently 0x01)
//   [5]     Subtype                    (see P2PSubtype)
//   [6..]   Subtype-specific payload   (all integers little-endian, ASCII strings)
//
// Control packets are NEVER bitmask-compressed. The receiver checks the magic
// before attempting game decompression; a 4-byte magic plus a version byte in
// a known-good range plus a valid subtype makes an accidental collision with a
// compressed game frame astronomically unlikely (documented residual risk in
// docs/P2P_HOLE_PUNCHING.md).
//
// Little-endian is used throughout to match the existing MessageSerializer.

using System.Buffers.Binary;
using System.Net;
using System.Text;

namespace OVS.Rollback.P2P
{
    /// <summary>Control message subtypes. Client->Server use 0x01..0x7F, Server->Client use 0x80..0xFF.</summary>
    public enum P2PSubtype : byte
    {
        // ── Client → Server ──
        Register = 0x01,      // announce presence on the control channel; server records reflexive endpoint
        PunchResult = 0x02,   // report whether a direct hole to a given peer succeeded
        RelayData = 0x03,     // TURN: opaque bytes to be forwarded to a destination peer
        KeepAlive = 0x04,     // refresh NAT binding + liveness

        // ── Server → Client ──
        RegisterAck = 0x81,   // reflexive endpoint echo + assigned role + mode
        PeerList = 0x82,      // every peer's reflexive endpoint + role
        PunchNow = 0x83,      // synchronized punch start signal
        RelayDeliver = 0x84,  // TURN: opaque bytes forwarded from a source peer
        UseRelay = 0x85,      // route this peer via server relay (punch failed)
        UseDirect = 0x86,     // route this peer directly at the given endpoint (punch succeeded)
    }

    /// <summary>Parsed client→server control message. Only the fields relevant to the subtype are populated.</summary>
    public readonly struct P2PInbound
    {
        public P2PSubtype Subtype { get; init; }
        public ushort PlayerIndex { get; init; }
        public ushort PeerIndex { get; init; }
        public bool Success { get; init; }
        public string MatchId { get; init; }
        public string Key { get; init; }
        public ushort DstPlayerIndex { get; init; }
        public ReadOnlyMemory<byte> Data { get; init; }
    }

    public static class P2PControl
    {
        public static ReadOnlySpan<byte> Magic => "OVSP"u8;
        public const byte Version = 0x01;
        public const int HeaderSize = 6; // magic(4) + version(1) + subtype(1)

        /// <summary>Fast check: does this raw datagram look like a P2P control packet (vs. a game frame)?</summary>
        public static bool IsControlPacket(ReadOnlySpan<byte> buffer)
        {
            if (buffer.Length < HeaderSize) return false;
            if (!buffer[..4].SequenceEqual(Magic)) return false;
            if (buffer[4] != Version) return false;
            byte sub = buffer[5];
            // Accept only known subtypes so a stray magic-shaped game frame is rejected.
            return Enum.IsDefined(typeof(P2PSubtype), sub);
        }

        // ═══════════════════════════════════════════
        //  Parse (client → server)
        // ═══════════════════════════════════════════

        /// <summary>Parse a control datagram. Returns null if malformed. Caller must have passed IsControlPacket first.</summary>
        public static P2PInbound? Parse(ReadOnlySpan<byte> buffer)
        {
            if (buffer.Length < HeaderSize) return null;
            var subtype = (P2PSubtype)buffer[5];
            int o = HeaderSize;

            try
            {
                switch (subtype)
                {
                    case P2PSubtype.Register:
                    {
                        ushort idx = ReadU16(buffer, ref o);
                        string matchId = ReadStr8(buffer, ref o);
                        string key = ReadStr8(buffer, ref o);
                        return new P2PInbound { Subtype = subtype, PlayerIndex = idx, MatchId = matchId, Key = key };
                    }
                    case P2PSubtype.PunchResult:
                    {
                        ushort idx = ReadU16(buffer, ref o);
                        ushort peer = ReadU16(buffer, ref o);
                        byte ok = buffer[o++];
                        return new P2PInbound { Subtype = subtype, PlayerIndex = idx, PeerIndex = peer, Success = ok != 0 };
                    }
                    case P2PSubtype.RelayData:
                    {
                        ushort idx = ReadU16(buffer, ref o);
                        ushort dst = ReadU16(buffer, ref o);
                        ushort len = ReadU16(buffer, ref o);
                        if (o + len > buffer.Length) return null;
                        var data = buffer.Slice(o, len).ToArray();
                        return new P2PInbound { Subtype = subtype, PlayerIndex = idx, DstPlayerIndex = dst, Data = data };
                    }
                    case P2PSubtype.KeepAlive:
                    {
                        ushort idx = ReadU16(buffer, ref o);
                        return new P2PInbound { Subtype = subtype, PlayerIndex = idx };
                    }
                    default:
                        return null; // server→client subtype received on the server; ignore
                }
            }
            catch (ArgumentOutOfRangeException)
            {
                return null;
            }
        }

        // ═══════════════════════════════════════════
        //  Build (server → client)
        // ═══════════════════════════════════════════

        public static byte[] BuildRegisterAck(PeerRole role, P2PMode mode, IPEndPoint reflexive)
        {
            var ip = reflexive.Address.ToString();
            var ipBytes = Encoding.ASCII.GetBytes(ip);
            var buf = new byte[HeaderSize + 1 + 1 + 1 + ipBytes.Length + 2];
            int o = WriteHeader(buf, P2PSubtype.RegisterAck);
            buf[o++] = (byte)role;
            buf[o++] = (byte)mode;
            buf[o++] = (byte)ipBytes.Length;
            ipBytes.CopyTo(buf, o); o += ipBytes.Length;
            WriteU16(buf, ref o, (ushort)reflexive.Port);
            return buf;
        }

        public static byte[] BuildPeerList(IReadOnlyList<(ushort index, PeerRole role, IPEndPoint ep)> peers)
        {
            // size: header + count(1) + per peer [index(2)+role(1)+ipLen(1)+ip+port(2)]
            int size = HeaderSize + 1;
            var ipCache = new byte[peers.Count][];
            for (int i = 0; i < peers.Count; i++)
            {
                ipCache[i] = Encoding.ASCII.GetBytes(peers[i].ep.Address.ToString());
                size += 2 + 1 + 1 + ipCache[i].Length + 2;
            }
            var buf = new byte[size];
            int o = WriteHeader(buf, P2PSubtype.PeerList);
            buf[o++] = (byte)peers.Count;
            for (int i = 0; i < peers.Count; i++)
            {
                WriteU16(buf, ref o, peers[i].index);
                buf[o++] = (byte)peers[i].role;
                buf[o++] = (byte)ipCache[i].Length;
                ipCache[i].CopyTo(buf, o); o += ipCache[i].Length;
                WriteU16(buf, ref o, (ushort)peers[i].ep.Port);
            }
            return buf;
        }

        public static byte[] BuildPunchNow(ushort startDelayMs, byte attempts, ushort intervalMs)
        {
            var buf = new byte[HeaderSize + 2 + 1 + 2];
            int o = WriteHeader(buf, P2PSubtype.PunchNow);
            WriteU16(buf, ref o, startDelayMs);
            buf[o++] = attempts;
            WriteU16(buf, ref o, intervalMs);
            return buf;
        }

        public static byte[] BuildUseRelay(ushort peerIndex)
        {
            var buf = new byte[HeaderSize + 2];
            int o = WriteHeader(buf, P2PSubtype.UseRelay);
            WriteU16(buf, ref o, peerIndex);
            return buf;
        }

        public static byte[] BuildUseDirect(ushort peerIndex, IPEndPoint ep)
        {
            var ipBytes = Encoding.ASCII.GetBytes(ep.Address.ToString());
            var buf = new byte[HeaderSize + 2 + 1 + ipBytes.Length + 2];
            int o = WriteHeader(buf, P2PSubtype.UseDirect);
            WriteU16(buf, ref o, peerIndex);
            buf[o++] = (byte)ipBytes.Length;
            ipBytes.CopyTo(buf, o); o += ipBytes.Length;
            WriteU16(buf, ref o, (ushort)ep.Port);
            return buf;
        }

        public static byte[] BuildRelayDeliver(ushort srcPlayerIndex, ReadOnlySpan<byte> data)
        {
            var buf = new byte[HeaderSize + 2 + 2 + data.Length];
            int o = WriteHeader(buf, P2PSubtype.RelayDeliver);
            WriteU16(buf, ref o, srcPlayerIndex);
            WriteU16(buf, ref o, (ushort)data.Length);
            data.CopyTo(buf.AsSpan(o));
            return buf;
        }

        // ═══════════════════════════════════════════
        //  Primitive read/write helpers (little-endian)
        // ═══════════════════════════════════════════

        private static int WriteHeader(byte[] buf, P2PSubtype subtype)
        {
            Magic.CopyTo(buf);
            buf[4] = Version;
            buf[5] = (byte)subtype;
            return HeaderSize;
        }

        private static void WriteU16(byte[] buf, ref int o, ushort v)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(o), v);
            o += 2;
        }

        private static ushort ReadU16(ReadOnlySpan<byte> buf, ref int o)
        {
            ushort v = BinaryPrimitives.ReadUInt16LittleEndian(buf[o..]);
            o += 2;
            return v;
        }

        private static string ReadStr8(ReadOnlySpan<byte> buf, ref int o)
        {
            byte len = buf[o++];
            if (o + len > buf.Length) throw new ArgumentOutOfRangeException(nameof(len));
            var s = Encoding.ASCII.GetString(buf.Slice(o, len));
            o += len;
            return s;
        }
    }
}
