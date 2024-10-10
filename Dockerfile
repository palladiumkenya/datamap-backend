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
RUN apt-get update && apt-get install -y curl unixodbc

# Expose the port
EXPOSE 4142


# Command to run the application and start the cron job
CMD uvicorn main:app --host 0.0.0.0 --port 2122
