import pandas as pd
import sqlalchemy
import logging
from datetime import datetime

class DataInsert:
    """Processes data fetched from the database."""

    @staticmethod
    def prepare_dataframe(df: pd.DataFrame, max_version_value: int, room_type_id: int) -> pd.DataFrame:
        """Prepare and process data features for the dataframe."""
        try:
            df['created_by'] = None
            df['created_date'] = datetime.now()
            df['last_modified_by'] = None
            df['last_modified_date'] = None
            df['current_price'] = None
            df['source'] = None
            df['status'] = 'CALCULATED'
            df['version'] = max_version_value + 1
            df['building_id'] = None
            df['room_type_id'] = room_type_id
            df['hse'] = None

            df = df[['created_by', 'created_date', 'last_modified_by', 'last_modified_date',
                     'cf_adr_by_room', 'cf_occ', 'current_price', 'considered_date',
                     'dynamic_price', 'source', 'status', 'cf_occ_previous_week',
                     'version', 'building_id', 'hotel_id', 'room_type_id', 'hse', 'price_index']]

            df = df.rename(columns={
                            'cf_adr_by_room':'adr',
                            'cf_occ':'current_occupancy',
                            'considered_date':'date_day',
                            'cf_occ_previous_week':'previous_week_occupancy'
                            
            })
            df['dynamic_price'] = None
            logging.info("DataFrame prepared successfully.")
            return df
        except Exception as e:
            logging.error(f"Error in preparing DataFrame: {e}")
            raise

    @staticmethod
    def save_to_database(df: pd.DataFrame, connection_string: str = "postgresql://booking-user:FjQLgnQ26Kphpz82@104.248.44.194:5432/booking") -> None:
        """Save the DataFrame to the specified database."""
        try:
            df['created_date'] = pd.to_datetime(df['created_date'])
            df['adr'] = df['adr'].astype(float)
            df['current_occupancy'] = df['current_occupancy'].astype(float)
            df['date_day'] = pd.to_datetime(df['date_day'])
            #df['dynamic_price'] = df['dynamic_price'].astype(int)
            df['status'] = df['status'].astype(str)
            df['previous_week_occupancy'] = df['previous_week_occupancy'].astype(float)
            df['version'] = df['version'].astype(int)
            df['hotel_id'] = df['hotel_id'].astype(int)
            df['room_type_id'] = df['room_type_id'].astype(int)
            df['price_index'] = df['price_index'].astype(float)
            
            engine = sqlalchemy.create_engine(connection_string)
            with engine.begin() as conn:
                df.to_sql('hotel_dynamic_price', conn, if_exists='append', index=False)
            
            logging.info("DataFrame saved to database successfully.")
        except Exception as e:
            # Log detailed error information
            logging.error(f"Error in saving DataFrame to database: {e}")
            logging.debug("DataFrame causing error: %s", df)
            raise

    @staticmethod
    def save_to_rag(df: pd.DataFrame, connection_string: str = "postgresql://booking-user:FjQLgnQ26Kphpz82@104.248.44.194:5432/booking") -> None:
        """Save the DataFrame to the specified database."""
        try:
            engine = sqlalchemy.create_engine(connection_string)
            with engine.begin() as conn:
                df.to_sql('hotel_rag', conn, if_exists='append', index=False)

            logging.info("DataFrame saved to database successfully.")
        except Exception as e:
            # Log detailed error information
            logging.error(f"Error in saving DataFrame to database: {e}")
            logging.debug("DataFrame causing error: %s", df)
            raise

    @staticmethod
    def update_rag(df: pd.DataFrame, connection_string: str = "postgresql://booking-user:FjQLgnQ26Kphpz82@104.248.44.194:5432/booking") -> None:
        """Update the DataFrame to the specified database."""
        try:
            engine = sqlalchemy.create_engine(connection_string)
            with engine.begin() as conn:
                # Iterate over each row in the DataFrame
                for index, row in df.iterrows():
                    considered_date = row['considered_date']
                    hotel_id = row['hotel_id']
                    update_values = {col: row[col] for col in [
                        'net_room_revenue', 'no_rooms', 'inventory_rooms',
                        'cf_occ', 'cf_adr_by_room', 'revpar', 'days_prior',
                        'base_price', 'min_price', 'max_price', 'holiday_power',
                        'no_rooms_previous_year', 'cf_occ_previous_year',
                        'adr_previous_year', 'cf_occ_previous_week', 'daily_cagr',
                        'new_cluster', 'price_index', 'dynamic_price'
                    ]}

                    # Build the SET part of the query
                    set_query = ", ".join([f"{col} = :{col}" for col in update_values.keys()])

                    # Update the row where considered_date and hotel_id match
                    update_query = f"""
                    UPDATE hotel_rag 
                    SET {set_query}
                    WHERE considered_date = :considered_date AND hotel_id = :hotel_id
                    """
                    update_values['considered_date'] = considered_date
                    update_values['hotel_id'] = hotel_id

                    conn.execute(sqlalchemy.text(update_query), update_values)

            logging.info("DataFrame updated to database successfully.")
        except Exception as e:
            # Log detailed error information
            logging.error(f"Error in updating DataFrame to database: {e}")
            logging.debug("DataFrame causing error: %s", df)
            raise

    @staticmethod
    def check_hotel_id_exists(df: pd.DataFrame,
                              connection_string: str = "postgresql://booking-user:FjQLgnQ26Kphpz82@104.248.44.194:5432/booking") -> bool:
        """Check if a specific hotel_id exists in the hotel_rag table."""
        logging.info("Starting check for hotel_id existence.")
        try:
            engine = sqlalchemy.create_engine(connection_string)
            hotel_ids = df['hotel_id'].unique()
            # Convert numpy.int64 to Python int
            hotel_ids = [int(hotel_id) for hotel_id in hotel_ids]
            logging.info(f"Unique hotel_ids to check: {hotel_ids}")
            with engine.connect() as conn:
                for hotel_id in hotel_ids:
                    query = f"SELECT EXISTS (SELECT 1 FROM hotel_rag WHERE hotel_id = :hotel_id)"
                    result = conn.execute(sqlalchemy.text(query), {'hotel_id': hotel_id}).scalar()
                    if result:
                        logging.info(f"Hotel ID {hotel_id} exists in the hotel_rag table.")
                        return True
            logging.info("No hotel_id exists in the hotel_rag table.")
            return False
        except Exception as e:
            logging.error(f"Error in checking hotel_id existence: {e}")
            raise
