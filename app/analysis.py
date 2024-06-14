import pandas as pd
import numpy as np
import logging
from sklearn.preprocessing import MinMaxScaler
from sklearn.cluster import KMeans
from datetime import datetime

class Analysis:
    """Performs analysis on processed data."""

    @staticmethod
    def cagr_calculation(df: pd.DataFrame) -> pd.DataFrame:
        try:
            logging.info("Starting CAGR calculation.")
            df = df.sort_values(by='considered_date')
            df['no_rooms_growth_rate'] = df['no_rooms'].pct_change()
            df['no_rooms_growth_rate'].fillna(0, inplace=True)
            df['daily_cagr'] = (1 + df['no_rooms_growth_rate']) ** (1 / 1) - 1
            df['daily_cagr'] = df['daily_cagr'].fillna(0)
            df.replace([np.inf, -np.inf], 0, inplace=True)
            return df.drop('no_rooms_growth_rate', axis=1)
        except Exception as e:
            logging.error(f"Error in CAGR calculation: {e}")
            raise

    @staticmethod
    def clustering_analysis(df: pd.DataFrame, features: list, n_clusters: int = 3,
                            random_state: int = 42) -> pd.DataFrame:
        try:
            logging.info("Starting clustering analysis.")
            scaler = MinMaxScaler()
            df[features] = scaler.fit_transform(df[features])

            kmeans = KMeans(n_clusters=n_clusters, random_state=random_state)
            clusters = kmeans.fit_predict(df[features])
            df['cluster'] = clusters

            min_values = df.groupby('cluster')['revpar'].min().reset_index()
            min_values = min_values.sort_values('revpar')
            new_labels = {row.cluster: i + 1 for i, row in enumerate(min_values.itertuples())}
            df['new_cluster'] = df['cluster'].map(new_labels)
            df[features] = scaler.inverse_transform(df[features])
            logging.info("Clustering analysis completed successfully.")
            return df.drop('cluster', axis=1)
        except Exception as e:
            logging.error(f"Error in clustering analysis: {e}")
            raise

    @staticmethod
    def dynamic_price(df: pd.DataFrame, hotel_id: int,
                      features_to_normalize: list = ['cf_adr_by_room', 'cf_occ', 'adr_previous_year',
                                                     'cf_occ_previous_year', 'no_rooms_previous_year',
                                                     'cf_occ_previous_week', 'revpar', 'no_rooms',
                                                     'holiday_power', 'days_prior']) -> pd.DataFrame:
        try:
            logging.info("Starting dynamic price calculation.")
            scaler = MinMaxScaler()
            df[features_to_normalize] = scaler.fit_transform(df[features_to_normalize])

            # Calculate price index
            df['price_index'] = (
                    (df['cf_adr_by_room'] * 0.2) +
                    (df['adr_previous_year'] * 0.2) +
                    (df['cf_occ'] * 0.3) +
                    (df['revpar'] * 0.15) +
                    (df['new_cluster'] * 0.2) +
                    (df['cf_occ_previous_year'] * 0.2) +
                    (df['cf_occ_previous_week'] * 0.05) +
                    (df['no_rooms'] * 0.05) +
                    (df['no_rooms_previous_year'] * 0.02) +
                    (1 / (df['days_prior'] + 1) * 0.1) +
                    (df['daily_cagr'] * 0.03)
            )

            df['dynamic_price'] = (df['base_price'] * 0.4 + df['max_price'] * 0.5 + df['min_price'] * 0.1) * df[
                'price_index']
            df['dynamic_price'] = df['dynamic_price'].clip(lower=df['min_price'], upper=df['max_price'])
            df['dynamic_price'] = df['dynamic_price'].round()
            df[features_to_normalize] = scaler.inverse_transform(df[features_to_normalize])

            # Filter data for dates greater than or equal to current date
            current_date = pd.Timestamp(datetime.now().date())
            df = df[df['considered_date'] >= current_date]
            df['hotel_id'] = hotel_id
            logging.info("Dynamic price calculation completed successfully.")
            return df
        except Exception as e:
            logging.error(f"Error in dynamic price calculation: {e}")
            raise

    @staticmethod
    def convergence_seq(column: list, target: float = 0.9, min_index: float = 0.7, max_index: float = 1.13) -> None:
        next_terms = []

        for value in column:
            if value > max_index:
                r = 0.45
                next_term = target + abs(value - target) * r
            elif value < min_index:
                r = 0.35
                next_term = target - abs(value - target) * r
            else:
                next_term = value

            next_terms.append(next_term)

        return next_terms

    @staticmethod
    def apply_convergence_sequence(df: pd.DataFrame) -> None:
        try:
            logging.info("Applying convergence sequence.")
            column = df['price_index'].tolist()
            df['price_index'] = Analysis.convergence_seq(column)
            logging.info("Convergence sequence applied successfully.")
        except Exception as e:
            logging.error(f"Error in applying convergence sequence: {e}")
            raise
