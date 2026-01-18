import pandas as pd
import psycopg2
from sqlalchemy import create_engine

from ETL_Excercise.config.settings import USER, PASSWORD, HOST, PORT, DATABASE


def load_to_postgresql(df: pd.DataFrame, table_name: str, if_exists: str = "append"):
    try:
        connection_string = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
        engine = create_engine(connection_string)
        df.to_sql(table_name, engine, if_exists=if_exists, index=False)
        print(f"Successfully loaded DataFrame into PostgresSQL: {len(df)} rows into {table_name}")

    except Exception as e:
        print(f"Error loading DataFrame into PostgresSQL: {e}")
        raise

def load_transform_to_postgres(df: pd.DataFrame, table_name: str):
    create_table_sql = F"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            order_id INT PRIMARY KEY,
            customer_id INT,
            total_revenue NUMERIC(10, 2),
            profit_margin NUMERIC(10, 2),
            shipping_days INT
        );
    """

    insert_sql = f"""
        INSERT INTO {table_name} (order_id, customer_id, total_revenue, profit_margin, shipping_days)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (order_id) DO UPDATE 
        SET total_revenue = EXCLUDED.total_revenue,
            profit_margin = EXCLUDED.profit_margin,
            shipping_days = EXCLUDED.shipping_days;
    """

    db_params = {"host": HOST, "port": PORT, "database": DATABASE, "user": USER, "password": PASSWORD}

    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cursor:
                cursor.execute(create_table_sql)
                values = [
                    (
                        row["order_id"],
                        row["customer_id"],
                        row["total_revenue"],
                        row["profit_margin"],
                        row["shipping_days"]
                     )
                    for _, row in df.iterrows()
                ]
                cursor.executemany(insert_sql, values)
                conn.commit()
                print(f"Successfully created table: {len(df)} rows into table {table_name}")
    except Exception as e:
        print(f"Error loading DataFrame into PostgresSQL: {e}")
        raise