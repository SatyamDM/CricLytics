# Base image
FROM python:3.10-slim

# Install Java (required for Spark)
RUN apt-get update && \
    apt-get install -y default-jdk wget && \
    apt-get clean
# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Download Postgres JDBC Driver
RUN mkdir -p /opt/jars && \
    wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -P /opt/jars

# Add to Spark classpath
ENV SPARK_CLASSPATH=/opt/jars/postgresql-42.6.0.jar

# Copy project
WORKDIR /app
COPY . .

CMD ["python", "-m", "spark_jobs.silver.silver_build"]