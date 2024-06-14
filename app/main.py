from fastapi import FastAPI, HTTPException
from .hotel_pricing_system import HotelPricingSystem

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Welcome to the Hotel Pricing System API"}

@app.get("/fetch-data")
def fetch_data():
    try:
        df_hotel_booking, df_dp, occ_last, max_version_value = HotelPricingSystem.fetch_data()
        return {"status": "success", "data": "Data fetched successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/process-data")
def process_data():
    try:
        df_hotel_booking, df_dp, occ_last, max_version_value = HotelPricingSystem.fetch_data()
        df_2024, df_2024_hse = HotelPricingSystem.process_data(df_hotel_booking, df_dp, occ_last, max_version_value)
        return {"status": "success", "data": "Data processed successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/insert-data")
def insert_data():
    try:
        df_hotel_booking, df_dp, occ_last, max_version_value = HotelPricingSystem.fetch_data()
        df_2024, df_2024_hse = HotelPricingSystem.process_data(df_hotel_booking, df_dp, occ_last, max_version_value)
        HotelPricingSystem.insert_data(df_2024, df_2024_hse, max_version_value)
        return {"status": "success", "data": "Data inserted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
