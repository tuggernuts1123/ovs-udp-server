// OvsModels.cs
using System;
using System.Text.Json.Serialization;

namespace OVS.Rollback.Models
{
    public class OvsPlayer
    {
        [JsonPropertyName("player_index")]
        public ushort PlayerIndex { get; set; }

        [JsonPropertyName("player_id")]
        public string PlayerId { get; set; } = String.Empty;

        [JsonPropertyName("player_name")]
        public string PlayerName { get; set; } = String.Empty;

        [JsonPropertyName("player_character")]
        public string PlayerCharacter { get; set; } = String.Empty;

        [JsonPropertyName("ip")]
        public string Ip { get; set; } = String.Empty;

        [JsonPropertyName("is_host")]
        public bool IsHost { get; set; }

        [JsonPropertyName("is_spectator")]
        public bool IsSpectator { get; set; } = false;

        [JsonPropertyName("is_bot")]
        public bool IsBot { get; set; } = false;
    }

    public class OVSMatchConfig
    {
        [JsonPropertyName("max_players")]
        public int MaxPlayers { get; set; } = 6;

        [JsonPropertyName("match_duration")]
        public uint MatchDuration { get; set; } = 36000;

        [JsonPropertyName("players")]
        public List<OvsPlayer> Players { get; set; } = [];

        // P2P hole-punching mode for this match, chosen by the OVS backend:
        //   0 = Off (classic dedicated-server relay, default and unchanged)
        //   1 = Preferred (try direct peer paths, fall back to relay/authority)
        //   2 = Forced (direct only; no relay)
        // Absent in legacy configs → defaults to Off, so nothing changes unless
        // the backend explicitly opts a match in.
        //
        // Typed as int (not byte) and tolerant of stringly-typed numbers so a
        // malformed p2p_mode degrades to Off instead of throwing during config
        // deserialization — a throw here would abort the ENTIRE match, not just
        // disable P2P.
        [JsonPropertyName("p2p_mode")]
        [System.Text.Json.Serialization.JsonNumberHandling(
            System.Text.Json.Serialization.JsonNumberHandling.AllowReadingFromString)]
        public int P2PModeRaw { get; set; } = 0;

        [JsonIgnore]
        public P2P.P2PMode P2PMode =>
            Enum.IsDefined(typeof(P2P.P2PMode), (byte)(P2PModeRaw & 0xFF)) && P2PModeRaw is >= 0 and <= 2
                ? (P2P.P2PMode)P2PModeRaw
                : P2P.P2PMode.Off;

        public int NumSpectators
        {
            get {
                if (null == Players || Players.Count == 0)
                {
                    return 0;
                }

                int count = 0;
                foreach (var player in Players)
                {
                    // Recognize spectators by the explicit flag OR the
                    // PlayerIndex >= 8888 sentinel convention — matchmakers
                    // have used either. Missing a spectator here inflates
                    // TeamSlotCount and corrupts the wire-protocol slot count.
                    if (player.IsSpectator || player.PlayerIndex >= 8888)
                    {
                        count++;
                    }
                }
                return count;
            }
        }
        public int ActualPlayers => Players.Count - NumSpectators;

        public int NumBots
        {
            get {
                if (null == Players || Players.Count == 0) return 0;
                int count = 0;
                foreach (var player in Players)
                {
                    if (player.IsBot) count++;
                }
                return count;
            }
        }
    }
}
