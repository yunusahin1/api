import pandas as pd
import logging
import numpy as np
from datetime import datetime
class DataProcessing:
    """Processes data fetched from the database."""

    @staticmethod
    def processing(df: pd.DataFrame) -> pd.DataFrame:
        """Process data features."""
        try:
            df.fillna(0, inplace=True)
            df['days_prior'] = df['days_prior'].abs()
            df['no_rooms_previous_year'] = df.apply(lambda row: DataProcessing.get_previous_year_value(df, row, 'no_rooms'), axis=1)
            df['cf_occ_previous_year'] = df.apply(lambda row: DataProcessing.get_previous_year_value(df, row, 'cf_occ'), axis=1)
            df['adr_previous_year'] = df.apply(lambda row: DataProcessing.get_previous_year_value(df, row, 'cf_adr_by_room'), axis=1)
            logging.info("Columns are created.")
            return df
        except Exception as e:
            logging.error(f"Error in processing data features: {e}")
            raise

    @staticmethod
    def extract_date_features(df: pd.DataFrame) -> pd.DataFrame:
        """Extracts date features."""
        try:
            df['considered_date'] = pd.to_datetime(df['considered_date'])
            df['month'] = df['considered_date'].dt.month
            df['year'] = df['considered_date'].dt.year
            df['day'] = df['considered_date'].dt.day
            df['day_of_week'] = df['considered_date'].dt.dayofweek 
            df['week_number'] = df['considered_date'].dt.isocalendar().week
            logging.info("Date feature extraction completed.")
            return df
        except Exception as e:
            logging.error(f"Error in extracting date features: {e}")
            raise

    @staticmethod
    def filter_data(df: pd.DataFrame, year: int) -> pd.DataFrame:
        """Filters data for specific year."""
        try:
            df = df[df['year'] == year]
            logging.info(f"Data filtering for year {year} completed.")
            return df
        except Exception as e:
            logging.error(f"Error in data filtering: {e}")
            raise

    @staticmethod
    def get_previous_year_value(df: pd.DataFrame, row: pd.Series, column: str) -> float:
        """Get value of the given column from the previous year."""
        previous_year_data = df.loc[
            (df['week_number'] == row['week_number']) &
            (df['day_of_week'] == row['day_of_week']) &
            (df['considered_date'].dt.year == row['considered_date'].year - 1),
            column
        ]
        return previous_year_data.values[0] if not previous_year_data.empty else np.nan

    @staticmethod
    def merge_last_year_data(df: pd.DataFrame, last_year_file: str) -> pd.DataFrame:
        """Merge data with last year's data from an Excel file."""
        try:
            df_last = pd.read_excel(last_year_file)
            current_date = pd.Timestamp(datetime.now().date())
            df_last = df_last[df_last['considered_date'] >= current_date]
            df = pd.merge(df, df_last[['considered_date', 'cf_occ_previous_year', 'no_rooms_previous_year', 'adr_previous_year']],
                          on='considered_date', how='left')
            df.drop(columns=['cf_occ_previous_year_x', 'no_rooms_previous_year_x', 'adr_previous_year_x'], inplace=True)
            df.rename(columns={'cf_occ_previous_year_y': 'cf_occ_previous_year',
                               'no_rooms_previous_year_y': 'no_rooms_previous_year',
                               'adr_previous_year_y': 'adr_previous_year'}, inplace=True)
            logging.info("Data merged with last year's data.")
            return df
        except Exception as e:
            logging.error(f"Error in merging last year's data: {e}")
            raise
