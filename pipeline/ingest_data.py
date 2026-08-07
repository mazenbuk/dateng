import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm


DTYPE = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64",
}

PARSE_DATES = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
]

CSV_URL = (
    "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
    "yellow/yellow_tripdata_2021-01.csv.gz"
)

CHUNKSIZE = 100_000


@click.command()
@click.option("--pg-user", default="root", help="PostgreSQL user")
@click.option("--pg-pass", default="root", help="PostgreSQL password")
@click.option("--pg-host", default="localhost", help="PostgreSQL host")
@click.option("--pg-port", default=5432, type=int, help="PostgreSQL port")
@click.option("--pg-db", default="ny_taxi", help="PostgreSQL database name")
@click.option(
    "--target-table",
    default="yellow_taxi_data",
    help="Target table name",
)
@click.option(
    "--csv-url",
    default=CSV_URL,
    help="URL or local path to the CSV(.gz) file to ingest",
)
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, csv_url):
    """Ingest NYC taxi CSV data into Postgres in chunks."""

    # 1. Bikin koneksi ke Postgres
    engine = create_engine(
        f"postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    )

    # 2. Siapkan iterator pembaca CSV per chunk
    df_iter = pd.read_csv(
        csv_url,
        dtype=DTYPE,
        parse_dates=PARSE_DATES,
        iterator=True,
        chunksize=CHUNKSIZE,
    )

    # 3. Loop: chunk pertama bikin tabel dulu, lalu semua chunk di-insert
    first = True
    total_rows = 0

    for df_chunk in tqdm(df_iter, desc="Ingesting chunks"):
        if first:
            df_chunk.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists="replace",
            )
            first = False
            click.echo(f"Table '{target_table}' created.")

        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append",
        )
        total_rows += len(df_chunk)
        click.echo(f"Inserted chunk: {len(df_chunk)} rows (total: {total_rows})")

    click.echo(f"Done. Total rows ingested into '{target_table}': {total_rows}")


if __name__ == "__main__":
    run()