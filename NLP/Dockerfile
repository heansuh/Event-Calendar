# Use a lightweight Python image
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libffi-dev \
    libssl-dev \
    libjpeg-dev \
    zlib1g-dev \
    libpng-dev \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory inside the container
WORKDIR /app

# Copy the local files to the container
COPY . /app
COPY villages_min_5_poi.json /app/

# Install Python dependencies (spaCy and the large German model)
RUN pip install --no-cache-dir spacy python-dateutil \
    && python -m spacy download de_core_news_lg

# Set the default command to run the spacy_ner.py script
CMD ["python", "spacy_ner.py"]
