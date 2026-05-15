# Kafka File Relay

* Docker deployment (docker compose, bundled / external Kafka)
* Stateless Spring Boot backend (HTTP + SSE)
* Kafka as the data transport (KRaft), no DB
* Command Line tool

KafkaFile lets two users on different machines transfer a (potentially very large) file between
them. Each user runs the `kfile` CLI; both CLIs connect over HTTP/SSE to a shared backend.
The backend is the only component that talks to Kafka — it produces chunks from the sender
onto a `xfer.data` topic and consumes them on behalf of the receiver. Transfer state lives
in RAM on the backend and is persisted to a compacted `xfer.info` topic used as a
Write-Ahead Log for crash recovery.

The sender stays connected for the duration of the transfer (lazy mode). Chunks are 1 MB by
default with a sliding window of 10 in-flight chunks, ordered per transfer via a dedicated
Kafka partition acting as a transfer "slot". The `xfer.data` topic uses a short 30-second
retention — Kafka is the relay, not the durable buffer; on disruption the sender simply
re-produces from the last fsync'd offset. Transfers are resumable across CLI or backend
crashes, and verified end-to-end with SHA-256.

## Typical usage

- Install CLI: drop `kfile.jar` on the machine, create `~/.kfile/config.yaml` with
  `name`, `backend-url`, `bearer-token` (name set as alice)
- Receiver on machine B: `kfile receive --out-dir ./downloads` — waits for offers
- Sender on machine A: `kfile send --to bob ./bigfile.iso` — registers offer, streams file
- Receiver sees prompt: `Accept file 'bigfile.iso' (12.3 GB) from alice? [y/N]` (skip with `--silent`)
- Either side: `kfile cancel <transferId>` to abort


REST endpoints:
```
POST    /api/v1/transfers                       # sender: register offer, returns transferId
GET     /api/v1/transfers/{id}/events           # sender: SSE control stream
POST    /api/v1/transfers/{id}/chunks           # sender: upload chunk N (binary envelope)
DELETE  /api/v1/transfers/{id}                  # sender or receiver: cancel
GET     /api/v1/inbox/events?name={recipient}   # receiver: SSE stream of offers + chunk-available
POST    /api/v1/transfers/{id}/accept           # receiver: accept an offer
GET     /api/v1/transfers/{id}/chunks/{seq}     # receiver: download chunk N
POST    /api/v1/transfers/{id}/progress         # receiver: report lastFsyncedByteOffset
POST    /api/v1/transfers/{id}/complete         # receiver: report final sha256 verdict
GET     /api/v1/healthz                         # anyone: liveness (no auth)
```

## Additional info worth knowing

- **Concurrency ceiling** = `xfer.data` partition count (default 6). Each in-flight
  transfer occupies one partition slot.
- **Chunk size is capped** by Kafka's `max.message.bytes`.
- **Backend never uses `AdminClient`** — topics must be pre-created; cleanup is purely
  retention-driven (`retention.ms` on data, compaction on info).
- **Throughput** ≈ 10 - 20 MB/s aggregate.
