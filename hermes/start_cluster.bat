@echo off
echo Cleaning up old data...
rmdir /s /q c:\tmp\hermes-0 2>nul
rmdir /s /q c:\tmp\hermes-1 2>nul
rmdir /s /q c:\tmp\hermes-2 2>nul

echo Starting Node 0 (Leader/Seed)...
set HERMES_NODE_ID=hermes-0
set HERMES_LISTEN_ADDR=127.0.0.1:7001
set HERMES_HTTP_ADDR=127.0.0.1:7000
set HERMES_METRICS_ADDR=127.0.0.1:9000
set HERMES_DATA_DIR=c:\tmp\hermes-0
set HERMES_SEED_NODES=
start "Hermes Node 0" go run ./cmd/hermes-server server

timeout /t 2 /nobreak >nul

echo Starting Node 1...
set HERMES_NODE_ID=hermes-1
set HERMES_LISTEN_ADDR=127.0.0.1:7011
set HERMES_HTTP_ADDR=127.0.0.1:7010
set HERMES_METRICS_ADDR=127.0.0.1:9010
set HERMES_DATA_DIR=c:\tmp\hermes-1
set HERMES_SEED_NODES=127.0.0.1:7001
start "Hermes Node 1" go run ./cmd/hermes-server server

echo Starting Node 2...
set HERMES_NODE_ID=hermes-2
set HERMES_LISTEN_ADDR=127.0.0.1:7021
set HERMES_HTTP_ADDR=127.0.0.1:7020
set HERMES_METRICS_ADDR=127.0.0.1:9020
set HERMES_DATA_DIR=c:\tmp\hermes-2
set HERMES_SEED_NODES=127.0.0.1:7001
start "Hermes Node 2" go run ./cmd/hermes-server server

echo Cluster started! New windows should have opened.
echo To check status:
echo set HERMES_SERVER_ADDR=127.0.0.1:7001
echo go run ./cmd/hermes-cli cluster status
