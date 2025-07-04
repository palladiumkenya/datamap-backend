#!/bin/bash

export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
export PYTHONPATH=/app

cd /app  # Ensure working directory matches where .env is

{
  echo "========== $(date) =========="
  echo "Triggering load runner"
  /usr/local/bin/python /app/pipelines/load_pipeline_runner.py
  echo "============================="
} >> /luigi-logs/cron_luigi_load.log 2>&1