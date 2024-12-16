# Use an official Python runtime as a parent image
FROM python:3.10

# Set environment variables to avoid buffering and ensure a clean build
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Set the working directory inside the container
WORKDIR /app

# Copy only the requirements file first for better layer caching
COPY requirements.txt /app/

# Copy the finalized scrapers directory
COPY finalized_scrapers /app/finalized_scrapers

# Install Python dependencies
RUN pip install --no-cache-dir -r /app/requirements.txt

# Install prerequisites and dependencies for Chrome
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget unzip \
    fonts-liberation \
    libgtk-3-0 \
    libvulkan1 \
    libxfixes3 \
    xdg-utils \
    && wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get install -y ./google-chrome-stable_current_amd64.deb \
    && rm google-chrome-stable_current_amd64.deb \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Google Chrome
RUN wget -q https://storage.googleapis.com/chrome-for-testing-public/131.0.6778.87/linux64/chrome-linux64.zip && \
    unzip chrome-linux64.zip -d /usr/local/bin/ && \
    mv /usr/local/bin/chrome-linux64 /usr/local/bin/google-chrome && \
    chmod +x /usr/local/bin/google-chrome && \
    rm chrome-linux64.zip

# Install Chromedriver
RUN wget -q https://storage.googleapis.com/chrome-for-testing-public/131.0.6778.87/linux64/chromedriver-linux64.zip && \
    unzip chromedriver-linux64.zip && \
    mv chromedriver-linux64/chromedriver /usr/local/bin/ && \
    chmod +x /usr/local/bin/chromedriver && \
    rm -rf chromedriver-linux64 chromedriver-linux64.zip

# Add locale support for de_DE.UTF-8
RUN apt-get update && apt-get install -y locales && \
    sed -i '/^# *de_DE.UTF-8/s/^# //' /etc/locale.gen && \
    locale-gen && \
    update-locale LANG=de_DE.UTF-8

# Copy the rest of the application code
COPY . /app/

# Set the default command to run a simple HTTP server for Cloud Run compatibility
CMD ["python", "orchestrator.py"]

