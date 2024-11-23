FROM python:3.9-slim

# Set environment variables to optimize CPU performance in PyTorch
ENV OMP_NUM_THREADS=6
ENV MKL_NUM_THREADS=6

# Set the working directory
WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Download models during build to cache them in the Docker image
RUN python -c "from transformers import pipeline; pipeline('ner', model='lunesco/bert-german-ner'); pipeline('zero-shot-classification', model='MoritzLaurer/mDeBERTa-v3-base-mnli-xnli')"

# Copy the rest of the application files
COPY app.py .

# Expose Flask's default port
EXPOSE 5000

# Run the Flask app
CMD ["python", "app.py"]
