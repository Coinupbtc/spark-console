#!/usr/bin/env bash
# Collects GPU utilization + model names from DGX Spark via SSH
# Saves JSON array to /data/latest_data.json and appends to time series CSV
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_DIR="$SCRIPT_DIR/data"
CSV_FILE="$DATA_DIR/gpu_timeseries.csv"
LATEST_JSON="$DATA_DIR/latest_data.json"

mkdir -p "$DATA_DIR"

# Time info
TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
EPOCH=$(date +%s)
HOUR_24=$(date -u +%-H)
DAY_OF_WEEK=$(date -u +%w)  # 0=Sun

# SSH to DGX Spark and query GPU stats using nvidia-smi DCFM
# This queries all GPUs on the remote machine
GPU_JSON="["
FIRST=true

for gpu_id in $(seq 0 3); do
  # Query GPU utilization (power, memory, compute)
  QUERY=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no coinupbtc@192.168.50.123 \
    "nvidia-smi --query-gpu=index,utilization.gpu,power.draw,power.total_limit,memory.used,memory.total,temperature.gpu,fan.speed,pstate,name \
     --format=csv,noheader,nounits -i $gpu_id 2>/dev/null" || echo "FAILED")

  if [[ "$QUERY" == *"FAILED"* ]] || [[ -z "$QUERY" ]]; then
    # GPU unreachable or SSH failed — record zeros + error
    GPU_JSON+="{\"gpu_id\":$gpu_id,\"timestamp\":\"$TIMESTAMP\",\"epoch\":$EPOCH,'hour_24':$HOUR_24,'dow':$DAY_OF_WEEK,'utilization_gpu': 0,'power_watts': 0,'memory_used_mb': 0,'memory_total_mb': 0,'temperature_c': 0,'fan_speed_pct': 0,'pstate': '-','gpu_name': 'UNREACHABLE','model': 'N/A','error':'SSH or query failed'},"
    continue
  fi

  # Parse: index, util %, power draw, total power, mem used MB, mem total MB, temp C, fan%, state, name
  GPU_INDEX=$(echo "$QUERY" | cut -d',' -f1)
  UTIL_PCT=$(echo "$QUERY" | cut -d',' -f2)
  POWER_draw=$(echo "$QUERY" | cut -d',' -f3)
  POWER_total=$(echo "$QUERY" | cut -d',' -f4)
  MEM_USED=$(echo "$QUERY" | cut -d',' -f5)
  MEM_TOTAL=$(echo "$QUERY" | cut -d',' -f6)
  TEMP_C=$(echo "$QUERY" | cut -d',' -f7)
  FAN_PCT=$(echo "$QUERY" | cut -d',' -f8)
  PSTATE=$(echo "$QUERY" | cut -d',' -f9)
  GPU_NAME=$(echo "$QUERY" | cut -d',' -f10-)

  # Handle potential empty values — replace with "unknown" for safe JSON
  UTIL_PCT="${UTIL_PCT:-0}"
  POWER_draw="${POWER_draw:-0}"
  MEM_USED="${MEM_USED:-0}"
  TEMP_C="${TEMP_C:-0}"
  FAN_PCT="${FAN_PCT:-0}"
  PSTATE="${PSTATE:-unknown}"

  # Now query running processes to find what models are being used
  PROCESSES=$(ssh -o ConnectTimeout=5 coinupbtc@192.168.50.123 \
    "nvidia-smi --query-compute-apps=index,name,used_memory --format=csv,noheader,nounits -i $gpu_id 2>/dev/null" || true)

  model_names=""
  if [[ -n "$PROCESSES" ]] && ! echo "$PROCESSES" | grep -q "^$"; then
    # Format: pid, name, used_mem (comma separated per line)
    while IFS= read -r proc_line; do
      pname=$(echo "$proc_line" | cut -d',' -f2- | sed 's/\s*$//')
      if [[ -n "$pname" ]] && [[ "$pname" != " " ]]; then
        model_names+="${pname},"
      fi
    done <<< "$PROCESSES"
  fi
  model_names="${model_names%,}"  # Remove trailing comma
  [[ -z "$model_names" ]] && model_names="N/A"

  if [[ "$FIRST" == "true" ]]; then
    FIRST=false
  else
    GPU_JSON+=","
  fi
  
  GPU_JSON+="{\"gpu_id\":$gpu_id,\"timestamp\":\"$TIMESTAMP\",\"epoch\":$EPOCH,'hour_24':$HOUR_24,'dow':$DAY_OF_WEEK,\
'utilization_gpu':$UTIL_PCT,'power_watts':$(echo "$POWER_draw" | cut -d'.' -f1),'memory_used_mb':${MEM_USED:-0},\
'memory_total_mb':${MEM_total:-0},'temperature_c':${TEMP_C:-0},'fan_speed_pct':${FAN_PCT:-0},\
'pstate':'$PSTATE','gpu_name':'$GPU_NAME','model':'$model_names'},"
done

GPU_JSON="${GPU_JSON%,}"  # Remove trailing comma
GPU_JSON+="]"

# Save latest JSON snapshot
echo "$GPU_JSON" > "$LATEST_JSON"

# Append to time series CSV
echo "$TIMESTAMP,$EPOCH,$HOUR_24,$DAY_OF_WEEK,$GPU_JSON" >> "$CSV_FILE"

echo "Collected GPU data at $TIMESTAMP ($(date))"
echo "Saved to: $LATEST_JSON"
echo "Appended to: $CSV_FILE"
