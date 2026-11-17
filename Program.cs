// Cluster transaction counter monitor for RavenDB.
//
// Every RavenDB node keeps, per database, a counter of applied cluster-wide transaction commands. The
// counter must be equal on all nodes. This tool reads /admin/debug/cluster/history-logs from every node
// at the same time, takes the counter from each cluster transaction in the log (the number after "RAFT:"
// in the change vectors it produced), and compares the nodes at the same Raft index.
//
// It only sends GET requests. It changes nothing on the cluster. Settings are in config.json.

using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using System.Text.RegularExpressions;

var configPath = Path.Combine(AppContext.BaseDirectory, "config.json");
var config = JsonSerializer.Deserialize<Config>(File.ReadAllText(configPath),
    new JsonSerializerOptions { PropertyNameCaseInsensitive = true, ReadCommentHandling = JsonCommentHandling.Skip })!;
if (config.Nodes.Count < 2) throw new Exception("config.json: at least 2 nodes are needed");

var handler = new HttpClientHandler();
handler.ClientCertificates.Add(X509CertificateLoader.LoadPkcs12FromFile(config.ClientCertificatePath, config.ClientCertificatePassword));
if (config.TrustServerCertificate)
    handler.ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator;
var http = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(config.RequestTimeoutSeconds) };

var changeVectorRegex = new Regex("\"@change-vector\"\\s*:\\s*\"([^\"]*)\"");
var raftRegex = new Regex(@"RAFT:(\d+)-([^,\s]+)");
var logOptions = new JsonSerializerOptions
{
    DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull,
    Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping // keep "+" and "/" of database ids readable
};
var jsonOptions = new JsonSerializerOptions(logOptions) { WriteIndented = true };

var status = new Status { State = "Unknown", Reason = "The first check has not finished yet" };

// 1. The check loop.
_ = Task.Run(async () =>
{
    while (true)
    {
        try
        {
            status = await Check();
        }
        catch (Exception e)
        {
            status = new Status { State = "Unknown", CheckedAt = DateTime.UtcNow, Reason = e.Message };
        }
        Console.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss}Z {status.State}" +
                          (status.InSync ? "" : " " + JsonSerializer.Serialize(status, logOptions)));
        await Task.Delay(TimeSpan.FromSeconds(config.IntervalSeconds));
    }
});

// 2. The HTTP endpoint for the monitoring system: 200 when in sync, 503 otherwise.
var builder = WebApplication.CreateBuilder(new WebApplicationOptions { ContentRootPath = AppContext.BaseDirectory });
builder.Host.UseWindowsService(options => options.ServiceName = "ClusterTxSyncMonitor");
builder.Logging.ClearProviders();
builder.WebHost.UseUrls(config.ListenUrl);
var app = builder.Build();
app.MapGet("/", () => Results.Json(status, jsonOptions, statusCode: status.InSync ? 200 : 503));
Console.WriteLine($"Listening on {config.ListenUrl}, checking {config.Nodes.Count} nodes every {config.IntervalSeconds} s");
app.Run();

async Task<Status> Check()
{
    // Read all nodes at the same time, so that their logs cover the same Raft indexes.
    var reads = await Task.WhenAll(config.Nodes.Select(ReadNode));

    var errors = reads.Where(r => r.Error != null).ToDictionary(r => r.Node, r => r.Error!);
    if (errors.Count > 0)
        return new Status { State = "Unknown", CheckedAt = DateTime.UtcNow, Reason = "Could not read every node", Errors = errors };

    var outOfSync = new List<DatabaseResult>();
    var checkedDatabases = 0;
    foreach (var databaseId in reads.SelectMany(r => r.Counters.Keys).Distinct())
    {
        // Raft indexes for which every node has a counter for this database.
        var common = reads.Select(r => r.Counters.GetValueOrDefault(databaseId)?.Keys.AsEnumerable() ?? [])
            .Aggregate((a, b) => a.Intersect(b)).ToList();
        if (common.Count == 0)
            continue;
        checkedDatabases++;

        // Compare at the latest such index: that is the current state (older entries may predate a fix).
        var index = common.Max();
        var counters = reads.ToDictionary(r => r.Node, r => r.Counters[databaseId][index]);
        if (counters.Values.Distinct().Count() == 1)
            continue;

        outOfSync.Add(new DatabaseResult
        {
            Database = config.DatabaseNames.GetValueOrDefault(databaseId, databaseId),
            DatabaseId = databaseId,
            RaftIndex = index,
            Counters = counters,
            BehindHighest = counters.ToDictionary(c => c.Key, c => counters.Values.Max() - c.Value)
        });
    }

    if (outOfSync.Count > 0)
        return new Status { State = "OutOfSync", CheckedAt = DateTime.UtcNow, OutOfSync = outOfSync };
    if (checkedDatabases == 0)
        return new Status { State = "Unknown", CheckedAt = DateTime.UtcNow, Reason = "No cluster transaction is in the history logs of every node" };
    return new Status { State = "InSync", CheckedAt = DateTime.UtcNow };
}

async Task<NodeRead> ReadNode(NodeConfig node)
{
    var read = new NodeRead { Node = node.Name };
    try
    {
        using var response = await http.GetAsync(node.Url.TrimEnd('/') + "/admin/debug/cluster/history-logs");
        response.EnsureSuccessStatusCode();
        using var json = await JsonDocument.ParseAsync(await response.Content.ReadAsStreamAsync());

        foreach (var entry in json.RootElement.GetProperty("RachisLogHistory").EnumerateArray())
        {
            if (entry.GetProperty("Type").GetString() != "ClusterTransactionCommand")
                continue;
            var raftIndex = entry.GetProperty("Index").GetInt64();
            var result = entry.GetProperty("Result");
            var text = result.ValueKind == JsonValueKind.String ? result.GetString() : result.GetRawText();

            // Each change vector the transaction produced has "RAFT:<counter>-<database id>".
            // Keep the lowest counter per database for this transaction.
            foreach (Match changeVector in changeVectorRegex.Matches(text ?? ""))
            foreach (Match raft in raftRegex.Matches(changeVector.Groups[1].Value))
            {
                var counter = long.Parse(raft.Groups[1].Value);
                var perIndex = read.Counters.TryGetValue(raft.Groups[2].Value, out var p) ? p : read.Counters[raft.Groups[2].Value] = new();
                perIndex[raftIndex] = perIndex.TryGetValue(raftIndex, out var existing) ? Math.Min(existing, counter) : counter;
            }
        }
    }
    catch (Exception e)
    {
        read.Error = e.Message;
    }
    return read;
}

class Config
{
    public List<NodeConfig> Nodes { get; set; } = [];
    public string ClientCertificatePath { get; set; } = "";
    public string? ClientCertificatePassword { get; set; }
    public bool TrustServerCertificate { get; set; }
    public int IntervalSeconds { get; set; } = 60;
    public int RequestTimeoutSeconds { get; set; } = 60;
    public string ListenUrl { get; set; } = "http://localhost:5080";
    public Dictionary<string, string> DatabaseNames { get; set; } = [];
}

class NodeConfig
{
    public string Name { get; set; } = "";
    public string Url { get; set; } = "";
}

class NodeRead
{
    public string Node = "";
    public string? Error;
    public Dictionary<string, Dictionary<long, long>> Counters = []; // database id -> Raft index -> counter
}

class Status
{
    public bool InSync => State == "InSync";
    public string State { get; set; } = "";
    public DateTime? CheckedAt { get; set; }
    public string? Reason { get; set; }
    public Dictionary<string, string>? Errors { get; set; }
    public List<DatabaseResult>? OutOfSync { get; set; }
}

class DatabaseResult
{
    public string Database { get; set; } = "";
    public string DatabaseId { get; set; } = "";
    public long RaftIndex { get; set; }
    public Dictionary<string, long> Counters { get; set; } = [];
    public Dictionary<string, long> BehindHighest { get; set; } = [];
}
