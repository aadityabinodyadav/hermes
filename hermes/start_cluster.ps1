# Starts a 3-node Hermes cluster locally

Write-Host "Cleaning up old data..."
if (Test-Path "/tmp/hermes-0") { Remove-Item -Recurse -Force "/tmp/hermes-0" }
if (Test-Path "/tmp/hermes-1") { Remove-Item -Recurse -Force "/tmp/hermes-1" }
if (Test-Path "/tmp/hermes-2") { Remove-Item -Recurse -Force "/tmp/hermes-2" }

Write-Host "Starting Node 0 (Leader/Seed)..."
Start-Process -NoNewWindow -FilePath "go" -ArgumentList "run ./cmd/hermes-server server" -Environment @{
    HERMES_NODE_ID="hermes-0";
    HERMES_LISTEN_ADDR="127.0.0.1:7001";
    HERMES_HTTP_ADDR="127.0.0.1:7000";
    HERMES_METRICS_ADDR="127.0.0.1:9000";
    HERMES_DATA_DIR="/tmp/hermes-0"
}

Start-Sleep -Seconds 2

Write-Host "Starting Node 1..."
Start-Process -NoNewWindow -FilePath "go" -ArgumentList "run ./cmd/hermes-server server" -Environment @{
    HERMES_NODE_ID="hermes-1";
    HERMES_LISTEN_ADDR="127.0.0.1:7011";
    HERMES_HTTP_ADDR="127.0.0.1:7010";
    HERMES_METRICS_ADDR="127.0.0.1:9010";
    HERMES_DATA_DIR="/tmp/hermes-1";
    HERMES_SEED_NODES="127.0.0.1:7001"
}

Write-Host "Starting Node 2..."
Start-Process -NoNewWindow -FilePath "go" -ArgumentList "run ./cmd/hermes-server server" -Environment @{
    HERMES_NODE_ID="hermes-2";
    HERMES_LISTEN_ADDR="127.0.0.1:7021";
    HERMES_HTTP_ADDR="127.0.0.1:7020";
    HERMES_METRICS_ADDR="127.0.0.1:9020";
    HERMES_DATA_DIR="/tmp/hermes-2";
    HERMES_SEED_NODES="127.0.0.1:7001"
}

Write-Host "Cluster started in background! Check status with:"
Write-Host "HERMES_LISTEN_ADDR=127.0.0.1:7001 go run ./cmd/hermes-cli cluster status"
