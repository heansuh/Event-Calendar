# Use the official Python image as the base
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the main script and scrapers directory into the container
COPY main.py .
COPY scrapers ./scrapers

# Set the default command to run the main script
CMD ["python", "main.py"]
