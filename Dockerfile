# Use a slim Python base
FROM python:3.12-slim

# Set working dir
WORKDIR /app

# Copy and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your application code
COPY . .

# Expose port 5000 (Flask default)
EXPOSE 5000

# Environment variables—Flask in production mode
ENV FLASK_APP=app/webapp.py:create_app
ENV FLASK_ENV=production

# Install Gunicorn
# expose the port
EXPOSE 5000

# run with Gunicorn against wsgi:app
CMD ["gunicorn", "-b", "0.0.0.0:5000", "--timeout", "120", "wsgi:app"]



