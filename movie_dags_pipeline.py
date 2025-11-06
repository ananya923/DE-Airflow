from __future__ import annotations
import os
import csv
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
from airflow import DAG
from airflow.sdk import task
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ────────────────────────────────────────────────
# Configuration
# ────────────────────────────────────────────────
OUTPUT_DIR = "/opt/airflow/data/movies"
os.makedirs(OUTPUT_DIR, exist_ok=True)

TARGET_TABLE = "movies_final"
SCHEMA = "week9_movies"

default_args = {"owner": "student", "retries": 1, "retry_delay": timedelta(minutes=2)}

# ────────────────────────────────────────────────
# DAG Definition
# ────────────────────────────────────────────────
with DAG(
    dag_id="movie_pipeline",
    start_date=datetime(2025, 10, 1),
    schedule="@once",
    catchup=False,
    default_args=default_args,
    description="Movie data ingestion, transformation, merge, analysis, and cleanup pipeline",
) as dag:

    # ────────────────────────────────────────────────
    # Data Ingestion (Parallel)
    # ────────────────────────────────────────────────
    @task()
    def fetch_movies(output_dir: str = OUTPUT_DIR) -> str:
        """Create or load a simple movies dataset."""
        data = [
            {"movie_id": 1, "title": "Inception", "genre": "Sci-Fi", "year": 2010},
            {
                "movie_id": 2,
                "title": "The Dark Knight",
                "genre": "Action",
                "year": 2008,
            },
            {"movie_id": 3, "title": "Interstellar", "genre": "Sci-Fi", "year": 2014},
            {"movie_id": 4, "title": "Tenet", "genre": "Action", "year": 2020},
            {"movie_id": 5, "title": "Dunkirk", "genre": "War", "year": 2017},
        ]
        path = os.path.join(output_dir, "movies.csv")
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        return path

    @task()
    def fetch_ratings(output_dir: str = OUTPUT_DIR) -> str:
        """Create or load a ratings dataset."""
        data = [
            {"movie_id": 1, "user_id": 101, "rating": 9.0},
            {"movie_id": 2, "user_id": 102, "rating": 9.5},
            {"movie_id": 3, "user_id": 103, "rating": 8.8},
            {"movie_id": 4, "user_id": 104, "rating": 7.5},
            {"movie_id": 5, "user_id": 105, "rating": 8.0},
        ]
        path = os.path.join(output_dir, "ratings.csv")
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        return path

    # ────────────────────────────────────────────────
    # Transformations (in parallel using TaskGroup)
    # ────────────────────────────────────────────────
    with TaskGroup("transformations") as transformations:

        @task()
        def transform_movies(movies_path: str) -> str:
            df = pd.read_csv(movies_path)
            df["decade"] = (df["year"] // 10) * 10
            path = os.path.join(OUTPUT_DIR, "movies_transformed.csv")
            df.to_csv(path, index=False)
            return path

        @task()
        def transform_ratings(ratings_path: str) -> str:
            df = pd.read_csv(ratings_path)
            df["rating_category"] = pd.cut(
                df["rating"], bins=[0, 7, 8.5, 10], labels=["Low", "Medium", "High"]
            )
            path = os.path.join(OUTPUT_DIR, "ratings_transformed.csv")
            df.to_csv(path, index=False)
            return path

        movies_transformed = transform_movies(fetch_movies())
        ratings_transformed = transform_ratings(fetch_ratings())

    # ────────────────────────────────────────────────
    # Merge and Load to Postgres
    # ────────────────────────────────────────────────
    @task()
    def merge_datasets(movies_path: str, ratings_path: str) -> str:
        df_movies = pd.read_csv(movies_path)
        df_ratings = pd.read_csv(ratings_path)
        merged = pd.merge(df_movies, df_ratings, on="movie_id", how="inner")
        merged_path = os.path.join(OUTPUT_DIR, "merged_movies.csv")
        merged.to_csv(merged_path, index=False)
        return merged_path

    @task()
    def load_to_postgres(csv_path: str, conn_id="Postgres") -> int:
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        df = pd.read_csv(csv_path)
        columns = df.columns.tolist()

        create_schema = f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};"
        create_table = f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.{TARGET_TABLE} (
            {', '.join([f'{col} TEXT' for col in columns])}
        );
        """

        insert_sql = f"""
        INSERT INTO {SCHEMA}.{TARGET_TABLE} ({', '.join(columns)})
        VALUES ({', '.join(['%s']*len(columns))});
        """

        with conn.cursor() as cur:
            cur.execute(create_schema)
            cur.execute(create_table)
            for _, row in df.iterrows():
                cur.execute(insert_sql, tuple(map(str, row)))
            conn.commit()
        conn.close()
        return len(df)

    # ────────────────────────────────────────────────
    # Analysis & Visualization
    # ────────────────────────────────────────────────
    @task()
    def analyze_and_visualize(csv_path: str) -> str:
        df = pd.read_csv(csv_path)
        output_dir = os.path.join(OUTPUT_DIR, "visuals")
        os.makedirs(output_dir, exist_ok=True)

        # Plot average rating per genre
        genre_avg = df.groupby("genre")["rating"].mean()
        plt.figure(figsize=(6, 4))
        genre_avg.plot(kind="bar", title="Average Rating by Genre")
        plt.ylabel("Average Rating")
        plt.tight_layout()
        path = os.path.join(output_dir, "avg_rating_by_genre.png")
        plt.savefig(path)
        plt.close()

        print(f"Visualization saved to {path}")
        return path

    # ────────────────────────────────────────────────
    # Cleanup
    # ────────────────────────────────────────────────
    @task()
    def cleanup_folder(folder: str = OUTPUT_DIR):
        for file in os.listdir(folder):
            if file.endswith(".csv"):
                os.remove(os.path.join(folder, file))
        print("Intermediate CSV files cleaned up.")

    # ────────────────────────────────────────────────
    # Pipeline Orchestration
    # ────────────────────────────────────────────────
    merged_path = merge_datasets(movies_transformed, ratings_transformed)
    load_result = load_to_postgres(merged_path)
    visualization = analyze_and_visualize(merged_path)
    cleanup = cleanup_folder()

    # Task dependencies
    [movies_transformed, ratings_transformed] >> merged_path
    merged_path >> load_result >> visualization >> cleanup
