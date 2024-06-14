FROM ubuntu:latest

RUN apt-get update && apt-get install -y python3-pip python3-venv

WORKDIR /app

COPY requirements.txt requirements.txt

RUN python3 -m venv /app/venv
RUN /app/venv/bin/pip install --upgrade pip
RUN /app/venv/bin/pip install -r requirements.txt

COPY . .

CMD ["/app/venv/bin/uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
