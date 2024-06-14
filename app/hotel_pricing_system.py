import pandas as pd
import logging
from .db_connection import DBConnection
from .data_processing import DataProcessing
from .analysis import Analysis
from .db_config import get_db_config
from .queries import queries, hotel_id, room_type_id
from .db_insert import DataInsert

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class HotelPricingSystem:
    """Main system for handling hotel pricing operations."""

    @staticmethod
    def get_db_config():
        """Fetches database configuration."""
        return get_db_config()

    @staticmethod
    def fetch_data():
        """Fetches necessary data from the database."""
        try:
            db_config = HotelPricingSystem.get_db_config()
            db_conn = DBConnection(**db_config)
            df_hotel_booking = db_conn.fetch_data(queries["hotel_booking"])
            df_max_version = db_conn.fetch_data(queries["max_version"])
            max_version_value = df_max_version['max_version'][0]
            df_dp = db_conn.fetch_data(queries["dp"])
            occ_last = db_conn.fetch_data(queries["occupancy_last_week"])[['date_day', 'occupancy_rates_past']]
            occ_last['date_day'] = pd.to_datetime(occ_last['date_day'])
            logging.info("Data fetching completed.")
            return df_hotel_booking, df_dp, occ_last, max_version_value
        except Exception as e:
            logging.error(f"Error fetching data: {e}")
            raise

    @staticmethod
    def process_data(df_hotel_booking, df_dp, occ_last, max_version_value):
        """Processes the fetched data."""
        try:
            processor = DataProcessing()
            analyzer = Analysis()

            df = processor.extract_date_features(df_hotel_booking)
            df = processor.processing(df)
            df = df.merge(occ_last, how='left', left_on='considered_date', right_on='date_day')
            df = df.rename(columns={'occupancy_rates_past': 'cf_occ_previous_week'})
            df = df.drop(columns=['date_day'])
            df_2024 = processor.filter_data(df, 2024)
            df_2024 = analyzer.cagr_calculation(df_2024)
            df_2024 = analyzer.clustering_analysis(df=df_2024, features=['revpar'])
            df_2024_hse = analyzer.dynamic_price(df_2024.copy(), hotel_id).reset_index(drop=True)
            df_2024_hse['dynamic_price'] = df_dp['dynamic_price']
            df_2024_hse['price_index'] = df_dp['price_index']
            df_2024 = analyzer.dynamic_price(df_2024, hotel_id)
            control = df_2024.copy()
            control['base_vs_dynamic'] = (control['dynamic_price'] / control['base_price']) - 1
            logging.info("Data processing completed.")

            df_2024.to_excel('test.xlsx', index=False)

            return df_2024, df_2024_hse
        except Exception as e:
            logging.error(f"Error processing data: {e}")
            raise

    @staticmethod
    def insert_data(df_2024, df_2024_hse, max_version_value):
        """Inserts the processed data back into the database."""
        try:
            inserter = DataInsert()
            if inserter.check_hotel_id_exists(df_2024_hse):
                inserter.update_rag(df_2024_hse)
            else:
                inserter.save_to_rag(df_2024_hse)

            df_2024 = inserter.prepare_dataframe(df_2024, max_version_value, room_type_id)
            inserter.save_to_database(df_2024)
            logging.info("Data insertion completed.")
        except Exception as e:
            logging.error(f"Error inserting data: {e}")
            raise
