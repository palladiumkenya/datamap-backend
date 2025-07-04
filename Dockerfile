# Base image
FROM python:3.11-slim

#Specifying timezone
ENV TZ=Africa/Nairobi

# Install cron and git
RUN apt-get update && apt-get install -y git python3-dev default-libmysqlclient-dev build-essential pkg-config

# Set the working directory
WORKDIR /app

# Copy the requirements file
COPY requirements.txt .

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Install curl and ODBC
RUN apt-get update && apt-get install -y curl unixodbc cron

## Copy cron job and register it
#COPY cron/crontab.txt /etc/cron.d/luigi-cron
#COPY cron/cronjob.sh /app/cronjob.sh
# Copy cron job file and luigi script
COPY cron/cronjob.txt /etc/cron.d/luigi-cron
COPY cron/run_luigi.sh /usr/local/bin/run_luigi.sh
COPY cron/run_luigi_send.sh /usr/local/bin/run_luigi_send.sh

# Set permissions
RUN chmod +x /usr/local/bin/run_luigi.sh && chmod +x /usr/local/bin/run_luigi_send.sh && chmod 0644 /etc/cron.d/luigi-cron

# Create log dir
RUN mkdir -p /luigi-logs

#RUN mkdir -p /luigi-state

# Expose the port
EXPOSE 4142


# Command to run the application and start the cron job
CMD uvicorn main:app --host 0.0.0.0 --port 4142
#CMD sh -c "cron && uvicorn main:app --host 0.0.0.0 --port 4142"
