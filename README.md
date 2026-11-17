# Cluster transaction sync monitor for RavenDB

A small console application that checks whether the cluster-wide transaction counters of a RavenDB cluster
are equal on every node, and serves the result over HTTP for a monitoring system.

It only reads: it sends `GET` requests to the nodes and changes nothing on the cluster.

## Quick start

1. Download the package for your platform from **Releases**:
   - `ClusterTxSyncMonitor-<version>-win-x64.zip` for Windows;
   - `ClusterTxSyncMonitor-<version>-linux-x64.zip` for Linux.

   Both are self-contained single files: no .NET installation is needed.
2. Unzip it into a folder, copy `config.example.json` to `config.json` in the same folder and fill it in
   (see [Configuration](#configuration)).
3. Run `ClusterTxSyncMonitor.exe` (Windows) or `./ClusterTxSyncMonitor` (Linux). It prints one line per check:

   ```
   Listening on http://localhost:5080, checking 3 nodes every 60 s
   2026-01-01 12:00:00Z InSync
   ```

4. Point the monitoring system at the `ListenUrl`, for example `http://localhost:5080/`.

To keep it running on Windows, register the same executable as a service (in an elevated command prompt; the
space after each `=` is required):

```
sc.exe create ClusterTxSyncMonitor binPath= "C:\ClusterTxSyncMonitor\ClusterTxSyncMonitor.exe" start= delayed-auto
sc.exe failure ClusterTxSyncMonitor reset= 86400 actions= restart/60000
sc.exe start ClusterTxSyncMonitor
```

On Linux, run it as a systemd unit.

## How it works

1. Every `IntervalSeconds` it sends `GET /admin/debug/cluster/history-logs` to all nodes at the same time.
2. Every RavenDB node keeps, per database, a counter of applied cluster-wide transaction commands, and puts it
   into the change vectors it generates as `RAFT:<counter>-<database id>`. From each cluster transaction in a
   node's log, the monitor takes that number.
3. For each database, it takes the latest Raft index that every node has in its log and compares the
   counters there. Equal on all nodes means in sync.
4. It serves the last result at `ListenUrl`.

## Output

`GET /` returns **HTTP 200** when in sync and **HTTP 503** otherwise, so a plain HTTP check is enough.

In sync:

```json
{
  "InSync": true,
  "State": "InSync",
  "CheckedAt": "2026-01-01T12:00:00Z"
}
```

Out of sync, with the counters at the compared Raft index and how far each node is behind the highest one:

```json
{
  "InSync": false,
  "State": "OutOfSync",
  "CheckedAt": "2026-01-01T12:00:00Z",
  "OutOfSync": [
    {
      "Database": "Orders",
      "DatabaseId": "AAAAAAAAAAAAAAAAAAAAAA",
      "RaftIndex": 1200,
      "Counters": { "A": 5000, "B": 4998, "C": 5000 },
      "BehindHighest": { "A": 0, "B": 2, "C": 0 }
    }
  ]
}
```

Could not decide (a node did not answer, or no cluster transaction is in the logs of every node yet):

```json
{
  "InSync": false,
  "State": "Unknown",
  "CheckedAt": "2026-01-01T12:00:00Z",
  "Reason": "Could not read every node",
  "Errors": { "C": "No connection could be made because the target machine actively refused it." }
}
```

## Configuration

`config.json` must be next to the executable. Everything specific to your cluster is in this file only.

| Setting | Meaning | Default |
|---|---|---|
| `Nodes` | Every node of the cluster: `Name` (used only in the output) and `Url`. | required |
| `ClientCertificatePath` | Client certificate (`.pfx`) with at least **Operator** access: the history log is an admin debug endpoint. | required |
| `ClientCertificatePassword` | Password of the `.pfx`. | none |
| `TrustServerCertificate` | Accept any server certificate. Only for test clusters with a self-signed certificate. | `false` |
| `IntervalSeconds` | Time between checks. | `60` |
| `RequestTimeoutSeconds` | Timeout of each request to a node. | `60` |
| `ListenUrl` | Where the status endpoint listens. `http://localhost:5080` is reachable only from this machine; use `http://0.0.0.0:5080` to reach it from elsewhere. | `http://localhost:5080` |
| `DatabaseNames` | Optional readable names for database ids in the output. The id is the part after `RAFT:<counter>-` in a document's change vector. | none |

The endpoint has no authentication. It shows only database ids or names, node names and counters. Keep it on
`localhost` or behind a firewall.

## Notes

- The history log holds the last 2,048 Raft commands of each node. A database is compared only when at least
  one of its cluster transactions is in that window on every node, so a database without recent cluster
  transactions is not checked.
- A difference shows up at the first cluster transaction of that database after it appeared.
- Reading the history log costs one request per node per check; at the default interval this is negligible.

## Building from source

Requires the .NET 10 SDK.

```
dotnet publish ClusterTxSyncMonitor.csproj -c Release -r win-x64 --self-contained -p:PublishSingleFile=true -o out
```

Use `-r linux-x64` for Linux. Pushing a tag such as `v1.0.1` builds both packages and publishes them as a
release (`.github/workflows/build.yml`).

## License

MIT, see [LICENSE](LICENSE).
