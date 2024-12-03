# Use the official Selenium standalone image with Firefox and GeckoDriver pre-installed
FROM selenium/standalone-firefox:latest

# Set the working directory inside the container
WORKDIR /app

# Copy the scraper script into the container
COPY biunsinnorden.py .

# Install Python and necessary dependencies
USER root
RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends python3 python3-pip \
    && pip3 install --break-system-packages selenium pandas \
    && rm -rf /var/lib/apt/lists/*

# Set the default command to run the scraper
CMD ["python3", "biunsinnorden.py"]
