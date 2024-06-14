FROM ubuntu:latest

# Install necessary packages including python3-venv
RUN apt-get update && apt-get install -y python3-pip python3-venv

WORKDIR /app

# Copy requirements.txt before creating the virtual environment
COPY requirements.txt requirements.txt

# Create a virtual environment
RUN python3 -m venv /app/venv

# Activate the virtual environment and install dependencies
RUN /app/venv/bin/pip install --upgrade pip
RUN /app/venv/bin/pip install -r requirements.txt

COPY . .

# Use the virtual environment's Python interpreter to run the app
CMD ["/app/venv/bin/uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
