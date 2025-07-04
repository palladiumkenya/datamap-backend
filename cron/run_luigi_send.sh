#!/bin/bash

export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
export PYTHONPATH=/app

cd /app  # Ensure working directory matches where .env is

{
  echo "========== $(date) =========="
  echo "Triggering send data to arehouse runner"
  /usr/local/bin/python /app/pipelines/send_pipeline_runner.py
  echo "============================="
} >> /luigi-logs/cron_luigi_send.log 2>&1