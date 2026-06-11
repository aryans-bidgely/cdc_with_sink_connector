# CDC Pipeline — Cassandra 4.0 + Debezium + Kafka (KRaft)

A working local CDC pipeline. Cassandra 4.0 → Debezium Server → Apache Kafka.  
No ZooKeeper. No auth. Just the pipeline running end to end.

---

## How it works

```
Cassandra 4.0  ──commit logs──►  Debezium Server  ──events──►  Kafka (KRaft)
                                 (Cassandra4Connector)              │
                                                                    ▼
                                                              Kafka UI
                                                         http://localhost:8080
```

**Key thing to understand about Cassandra CDC:**  
The Debezium Cassandra connector does **not** connect to Cassandra over the network to get changes.  
It reads Cassandra's **commit log files directly from disk**.  
That is why Debezium Server mounts the same Docker volume as Cassandra — it needs filesystem access to `/var/lib/cassandra/cdc_raw/`.

When you write to a CDC-enabled table, Cassandra writes to its commit log.  
When that log segment fills up (or you run `nodetool flush`), Cassandra moves it to `cdc_raw/`.  
Debezium picks it up, deserializes it, and produces a change event to Kafka.

---

## Stack

| Service | Image | Port |
|---|---|---|
| Kafka (KRaft) | `apache/kafka:3.8.0` | `29092` (host), `9092` (internal) |
| Kafka UI | `provectuslabs/kafka-ui:v0.7.2` | `8080` |
| Cassandra | `cassandra:4.0` (custom CDC patch) | `9042` |
| Debezium Server | `quay.io/debezium/server:3.2.5.Final` | `8088` (health) |

---

## Prerequisites

- Docker Desktop (or Docker Engine + Compose v2)
- At least **6 GB RAM** allocated to Docker  
  *(Cassandra alone wants 2 GB; 4 GB is comfortable)*
- VS Code with the **Docker** extension (optional but handy)

---

## Project structure

```
cdc-pipeline/
├── .env
├── docker-compose.yml
├── cassandra/
│   ├── Dockerfile              ← patches cassandra:4.0 to enable CDC
│   └── init/
│       └── 01-init.sh          ← creates keyspace + CDC-enabled tables
├── debezium/
│   └── conf/
│       └── application.properties
└── README.md
```

---

## Step 1 — Clone / create the project

You already have all the files. Open the `cdc-pipeline/` folder in VS Code.

```bash
cd cdc-pipeline
```

---

## Step 2 — Start the stack

```bash
docker compose up --build -d
```

This will:
1. Build the custom Cassandra image (patches `cassandra.yaml` to enable CDC)
2. Start Kafka in KRaft mode
3. Start Cassandra
4. Run `cassandra-init` (creates keyspace `testdb` with CDC-enabled tables)
5. Start Debezium Server

**Cassandra takes 60–90 seconds to fully start.**  
Watch it with:

```bash
docker compose logs -f cassandra
```

Wait until you see:
```
Starting listening for CQL clients on /0.0.0.0:9042
```

---

## Step 3 — Check everything is running

```bash
docker compose ps
```

All services should show `healthy` or `running`:
```
NAME              STATUS
cassandra         healthy
debezium-server   running
kafka             healthy
kafka-ui          running
cassandra-init    exited (0)   ← this is expected, it runs once and exits
```

Check Debezium started correctly:
```bash
docker compose logs debezium-server | tail -30
```

Look for:
```
Cassandra4Connector started
```

---

## Step 4 — Open Kafka UI

Go to **http://localhost:8080**

You should see:
- Cluster `local` connected
- Topics will appear here once CDC events are produced

---

## Step 5 — Write some data to Cassandra

Open a CQL shell:
```bash
docker compose exec cassandra cqlsh -e "USE testdb;"
```

Or open an interactive shell:
```bash
docker compose exec cassandra cqlsh
```

Then run:
```sql
USE testdb;

-- INSERT
INSERT INTO customers (id, first_name, last_name, email)
VALUES (5, 'Roger', 'Poor', 'roger@poor.com');

-- UPDATE
UPDATE customers SET first_name = 'Barry' WHERE id = 5;

-- DELETE
DELETE FROM customers WHERE id = 5;
```

---

## Step 6 — Flush Cassandra commit logs

**This step is required during testing.**

Cassandra only moves commit log segments to `cdc_raw/` when they are full (default 32 MB).  
Since you're only writing a few rows, that won't happen naturally.  
You must flush manually:

```bash
docker compose exec cassandra nodetool flush
```

Wait 2–3 seconds. Debezium will now pick up the CDC segments.

---

## Step 7 — See the events in Kafka

**Option A — Kafka UI (easiest)**

Go to **http://localhost:8080** → Topics → `cdc.testdb.customers` → Messages.

You'll see events like:
```json
{
  "after": {
    "id": 5,
    "first_name": "Roger",
    "last_name": "Poor",
    "email": "roger@poor.com"
  },
  "op": "c",
  "source": {
    "connector": "cassandra",
    "db": "testdb",
    "table": "customers"
  }
}
```

**Option B — Kafka console consumer**

```bash
docker compose exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --from-beginning \
  --property print.key=true \
  --topic cdc.testdb.customers
```

Press `Ctrl+C` to stop.

---

## Step 8 — Verify all topics

```bash
docker compose exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --list
```

Expected:
```
cdc.testdb.customers
cdc.testdb.orders
debezium.cassandra.offsets
```

---

## Stopping the pipeline

```bash
# Stop but keep data volumes (restart quickly next time)
docker compose down

# Stop AND delete everything (clean slate)
docker compose down -v
```

---

## Troubleshooting

### Nothing in Kafka after writing to Cassandra

**Most common cause:** commit log not flushed yet.

```bash
docker compose exec cassandra nodetool flush
```

Then wait 3–5 seconds and check Kafka UI again.

---

### Debezium logs show "cassandra.yaml not found"

The `cassandra_config` volume is written by the Cassandra container on startup.  
Debezium must start after Cassandra is healthy — the `depends_on` in docker-compose handles this.  
If Debezium started before Cassandra was ready, restart it:

```bash
docker compose restart debezium-server
```

---

### Cassandra init container failed

```bash
docker compose logs cassandra-init
```

If Cassandra wasn't fully ready, just re-run it:

```bash
docker compose run --rm cassandra-init
```

---

### Check CDC is enabled

```bash
docker compose exec cassandra grep -i cdc /etc/cassandra/cassandra.yaml
```

Should show:
```
cdc_enabled: true
cdc_raw_directory: /var/lib/cassandra/cdc_raw
```

---

### Check what's in cdc_raw

```bash
docker compose exec cassandra ls -lh /var/lib/cassandra/cdc_raw/
```

After `nodetool flush`, you'll see `.log` and `.idx` files here.  
Once Debezium processes them, they disappear.

---

### Debezium health check

```bash
curl http://localhost:8088/q/health
```

Should return `{"status":"UP"}`.

---

## Quick reference

```bash
# Start everything
docker compose up --build -d

# Watch logs
docker compose logs -f debezium-server
docker compose logs -f cassandra

# CQL shell
docker compose exec cassandra cqlsh

# Force flush (required after writes during testing)
docker compose exec cassandra nodetool flush

# Consume events from terminal
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --from-beginning \
  --topic cdc.testdb.customers

# Stop
docker compose down

# Wipe everything
docker compose down -v
```
