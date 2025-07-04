#!/bin/bash
mkdir -p /luigi-logs

cd /app
echo "$(date): Running Luigi pipeline..." >> /luigi-logs/cron.log
PYTHONPATH=. python pipelines/runner.py >> /luigi-logs/cron.log 2>&1