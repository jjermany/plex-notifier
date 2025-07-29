# syntax=docker/dockerfile:1

FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Set environment variables
ENV FLASK_APP=notifier_app.webapp:create_app
ENV PYTHONPATH=/app
ENV FLASK_RUN_HOST=0.0.0.0
ENV FLASK_ENV=production

# Expose port
EXPOSE 5000

# Run the Flask app
CMD ["gunicorn", "-b", "0.0.0.0:5000", "notifier_app.webapp:create_app()", "--workers", "1", "--threads", "4"]

