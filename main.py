import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
    
from app.hotel_pricing_system import HotelPricingSystem
####COMMENT
def run():
    df_hotel_booking, df_dp, occ_last, max_version_value = HotelPricingSystem.fetch_data()
    df_2024, df_2024_hse = HotelPricingSystem.process_data(df_hotel_booking, df_dp, occ_last, max_version_value)
    HotelPricingSystem.insert_data(df_2024, df_2024_hse, max_version_value)

if __name__ == "__main__":
    run()
