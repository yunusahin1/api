from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_read_root():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Welcome to the Hotel Pricing System API"}

def test_fetch_data():
    response = client.get("/fetch-data")
    assert response.status_code == 200
    assert response.json()["status"] == "success"

def test_process_data():
    response = client.post("/process-data")
    assert response.status_code == 200
    assert response.json()["status"] == "success"

def test_insert_data():
    response = client.post("/insert-data")
    assert response.status_code == 200
    assert response.json()["status"] == "success"
