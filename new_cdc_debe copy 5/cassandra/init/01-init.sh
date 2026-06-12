#!/bin/bash
set -e
 
echo "[init] Waiting for Cassandra..."
until cqlsh cassandra -e "DESCRIBE CLUSTER" > /dev/null 2>&1; do
    echo "[init] Not ready — retrying in 5s..."
    sleep 5
done
 
echo "[init] Creating schema and seed data..."
 
cqlsh cassandra << 'CQLEOF'
 
CREATE KEYSPACE IF NOT EXISTS bidgely
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
 
USE bidgely;
 
-- ═══════════════════════════════════════════════════════════════════════════
-- TABLE TYPE 1: EAV — Entity Attribute Value
-- One row per attribute. column2 = attribute name, value = typed blob.
-- blob encoding:
--   boolean false = 0x00, true = 0x01
--   int (32-bit big-endian): 10116 = 0x00002794
--   text = raw UTF-8 bytes
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS users (
    key     uuid,
    column1 varint,
    column2 text,
    value   blob,
    PRIMARY KEY (key, column1, column2)
) WITH cdc = true;
 
-- ─── User 1: ef9577a1 — pilot 10116 ──────────────────────────────────────
INSERT INTO users (key, column1, column2, value)
VALUES (ef9577a1-c09f-4ce4-a1a2-349a5b98bc3e, 0, 'pilotId', 0x00002784);
 
INSERT INTO users (key, column1, column2, value)
VALUES (ef9577a1-c09f-4ce4-a1a2-349a5b98bc3e, 0, 'notificationUserType', 0x44495341424c4544);
 
INSERT INTO users (key, column1, column2, value)
VALUES (ef9577a1-c09f-4ce4-a1a2-349a5b98bc3e, 0, 'userStatus', 0x454e41424c4544);
 
INSERT INTO users (key, column1, column2, value)
VALUES (ef9577a1-c09f-4ce4-a1a2-349a5b98bc3e, 0, 'partnerUserId', 0x313332383433383538395f313332343830343233365f31303332383638363330);
 
INSERT INTO users (key, column1, column2, value)
VALUES (ef9577a1-c09f-4ce4-a1a2-349a5b98bc3e, 0, 'userCreatedTime', 0x67FDAA42);
 
INSERT INTO users (key, column1, column2, value)
VALUES (ef9577a1-c09f-4ce4-a1a2-349a5b98bc3e, 0, 'isTestUser', 0x00);
 
INSERT INTO users (key, column1, column2, value)
VALUES (ef9577a1-c09f-4ce4-a1a2-349a5b98bc3e, 0, 'isSolarUser', 0x00);
 
INSERT INTO users (key, column1, column2, value)
VALUES (ef9577a1-c09f-4ce4-a1a2-349a5b98bc3e, 0, 'isResidentialUser', 0x00);
 
INSERT INTO users (key, column1, column2, value)
VALUES (ef9577a1-c09f-4ce4-a1a2-349a5b98bc3e, 0, 'isEmailidEmpty', 0x00);
 
INSERT INTO users (key, column1, column2, value)
VALUES (ef9577a1-c09f-4ce4-a1a2-349a5b98bc3e, 0, 'consentStatus', 0x4f425441494e4544);
 
-- ─── User 2: ab1234cd — pilot 10117 ──────────────────────────────────────
INSERT INTO users (key, column1, column2, value)
VALUES (ab1234cd-ef56-7890-abcd-ef1234567890, 0, 'pilotId', 0x00002785);
 
INSERT INTO users (key, column1, column2, value)
VALUES (ab1234cd-ef56-7890-abcd-ef1234567890, 0, 'notificationUserType', 0x454e41424c4544);
 
INSERT INTO users (key, column1, column2, value)
VALUES (ab1234cd-ef56-7890-abcd-ef1234567890, 0, 'userStatus', 0x454e41424c4544);
 
INSERT INTO users (key, column1, column2, value)
VALUES (ab1234cd-ef56-7890-abcd-ef1234567890, 0, 'partnerUserId', 0x313030305f323030305f33303030);
 
INSERT INTO users (key, column1, column2, value)
VALUES (ab1234cd-ef56-7890-abcd-ef1234567890, 0, 'userCreatedTime', 0x67FDAA42);
 
INSERT INTO users (key, column1, column2, value)
VALUES (ab1234cd-ef56-7890-abcd-ef1234567890, 0, 'isTestUser', 0x01);
 
INSERT INTO users (key, column1, column2, value)
VALUES (ab1234cd-ef56-7890-abcd-ef1234567890, 0, 'isSolarUser', 0x00);
 
INSERT INTO users (key, column1, column2, value)
VALUES (ab1234cd-ef56-7890-abcd-ef1234567890, 0, 'isResidentialUser', 0x01);
 
INSERT INTO users (key, column1, column2, value)
VALUES (ab1234cd-ef56-7890-abcd-ef1234567890, 0, 'isEmailidEmpty', 0x00);
 
INSERT INTO users (key, column1, column2, value)
VALUES (ab1234cd-ef56-7890-abcd-ef1234567890, 0, 'consentStatus', 0x4f425441494e4544);
 
 
-- ═══════════════════════════════════════════════════════════════════════════
-- TABLE TYPE 2: JSON_EMBEDDED
-- Normal composite PK + one text column holding JSON metrics
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS weather_data (
    country            text,
    zipcode            text,
    epoch_month        int,
    minute_since_epoch int,
    json_value         text,
    PRIMARY KEY ((country, zipcode, epoch_month), minute_since_epoch)
) WITH cdc = true;
 
INSERT INTO weather_data (country, zipcode, epoch_month, minute_since_epoch, json_value)
VALUES ('US', '90210', 202401, 730000,
'{"temp":"75.6","cdd":null,"visibility":"10.6","windGust":null,"spc":"17.01","maxTemp":null,"hdd":null,"dewPt":"72.2","windDir":"169.0","avgTemp":null,"relHum":"89.0","wetBulb":"73.2","minTemp":null,"windSpd":"1.7","feelsLike":"79.8","seaLvlPressure":"1000.4","prec":"0.06","skyCover":null,"snow":"0.0","cloudCeiling":"100.0"}');
 
INSERT INTO weather_data (country, zipcode, epoch_month, minute_since_epoch, json_value)
VALUES ('US', '10001', 202401, 730060,
'{"temp":"55.2","cdd":null,"visibility":"8.0","windGust":"12.5","spc":"5.10","maxTemp":null,"hdd":null,"dewPt":"40.1","windDir":"270.0","avgTemp":null,"relHum":"55.0","wetBulb":"45.2","minTemp":null,"windSpd":"8.3","feelsLike":"51.0","seaLvlPressure":"1013.2","prec":"0.0","skyCover":null,"snow":"0.0","cloudCeiling":"5000.0"}');
 
 
-- ═══════════════════════════════════════════════════════════════════════════
-- TABLE TYPE 3: PLAIN
-- Standard Cassandra table with regular typed columns.
-- Debezium emits native JSON types — no blob encoding, no EAV pivot needed.
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS pilot_info (
    pilot_id   int     PRIMARY KEY,
    pilot_name text,
    utility    text,
    region     text,
    is_active  boolean,
    created_at bigint
) WITH cdc = true;
 
INSERT INTO pilot_info (pilot_id, pilot_name, utility, region, is_active, created_at)
VALUES (10116, 'Solar Pilot Alpha', 'PGE', 'West', true, 1745230146);
 
INSERT INTO pilot_info (pilot_id, pilot_name, utility, region, is_active, created_at)
VALUES (10117, 'Solar Pilot Beta', 'SCE', 'South', false, 1745230200);
 
INSERT INTO pilot_info (pilot_id, pilot_name, utility, region, is_active, created_at)
VALUES (10118, 'Wind Pilot Gamma', 'SDGE', 'North', true, 1745230300);
 
CQLEOF
 
echo "[init] Done."
echo "[init]   users        : 2 rows seeded  (EAV — 10 attributes each)"
echo "[init]   weather_data : 2 rows seeded  (JSON_EMBEDDED)"
echo "[init]   pilot_info   : 3 rows seeded  (PLAIN)"