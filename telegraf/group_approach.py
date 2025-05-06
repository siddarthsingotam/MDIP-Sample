import os
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient
from collections import defaultdict
import psycopg2
import json
from psycopg2 import sql


# Load environment variables for InfluxDB credentials
KEY_PATH = r"..\keys\keys.env"
load_dotenv(KEY_PATH)

# -------- CONFIG --------
INFLUX_URL = "https://eu-central-1-1.aws.cloud2.influxdata.com/"
INFLUX_TOKEN = "XoIrNY-1EEPgYvuTi0QsxyB-KXhhX2sAbOD8XwXoP0T6dXsmBSU0loFiYWxICb3nrlU0nXnaSRZhQy7u_PE2Vg=="
INFLUX_ORG = "MDIP-Sample"
INFLUX_BUCKET = "Full_Player_Data"

PG_HOST = "pg-icehockey-player-metrics-andydeshko-a139.l.aivencloud.com"
PG_PORT = 25420
PG_DB = "defaultdb"
PG_USER = "avnadmin"
PG_PASSWORD = "AVNS_hECP7FXEij7npcktiKj"
# ------------------------


# Connect InfluxDB
try:
    influx = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    query_api = influx.query_api()
    print("✅ Connected to InfluxDB.")
except Exception as e:
    print(f"❌ Failed to connect to InfluxDB: {e}")
    exit(1)

# Connect PostgreSQL
try:
    pg_conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        sslmode='require'
    )
    pg_cursor = pg_conn.cursor()
    print("✅ Connected to PostgreSQL.")
except Exception as e:
    print(f"❌ Failed to connect to PostgreSQL: {e}")
    exit(1)

# Helper: infer PostgreSQL type from Python value
def infer_pg_type(value):
    if isinstance(value, bool):
        return 'BOOLEAN'
    elif isinstance(value, int):
        return 'BIGINT'
    elif isinstance(value, float):
        return 'DOUBLE PRECISION'
    elif isinstance(value, dict) or isinstance(value, list):
        return 'JSONB'
    else:
        return 'TEXT'

# Cache to track created tables
created_tables = {}

# Flux query (modify time range as needed)
flux_query = f'''
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -2d)
  |> filter(fn: (r) => exists r._value and exists r._field)
'''

tables = query_api.query(flux_query)

# Group records
grouped_data = defaultdict(lambda: defaultdict(dict))

for table in tables:
    for record in table.records:
        measurement = record.get_measurement()
        field = record.get_field()
        value = record.get_value()
        time = record.get_time()

        tags = record.values
        pico_id = tags.get("Pico_ID")
        player_id = tags.get("Player_ID")

        if pico_id is None or player_id is None:
            continue

        key = (measurement, time, pico_id, player_id)
        grouped_data[key][field] = value

# Step 2: Iterate grouped records and insert full rows
for (measurement, time, pico_id, player_id), fields_dict in grouped_data.items():
    # Ensure table and columns
    if measurement not in created_tables:
        # First pass to collect all fields in this measurement
        field_types = {}
        for field, value in fields_dict.items():
            field_types[field] = infer_pg_type(value)

        columns = [
            sql.SQL("time TIMESTAMPTZ"),
            sql.SQL("Pico_ID TEXT"),
            sql.SQL("Player_ID TEXT")
        ]
        for fname, ftype in field_types.items():
            columns.append(sql.SQL('"{}" {}').format(sql.SQL(fname), sql.SQL(ftype)))

        create_table_sql = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
            sql.Identifier(measurement),
            sql.SQL(", ").join(columns)
        )
        pg_cursor.execute(create_table_sql)
        pg_conn.commit()
        created_tables[measurement] = field_types
    else:
        field_types = created_tables[measurement]

    # Check and add missing columns dynamically
    for field, value in fields_dict.items():
        if field not in field_types:
            pg_cursor.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = %s AND column_name = %s
            """, (measurement.lower(), field.lower()))
            if pg_cursor.fetchone() is None:
                field_type = infer_pg_type(value)
                pg_cursor.execute(
                    sql.SQL('ALTER TABLE {table} ADD COLUMN "{field}" {type}').format(
                        table=sql.Identifier(measurement),
                        field=sql.SQL(field),
                        type=sql.SQL(field_type)
                    )
                )
                pg_conn.commit()
                print(f"➕ Added column '{field}' to table '{measurement}'")
                field_types[field] = field_type # update cache

    # Step 3: Prepare insert query with all columns
    columns = [sql.SQL("time"), sql.SQL("Pico_ID"), sql.SQL("Player_ID")]
    values = [time, pico_id, player_id]

    for field, field_type in field_types.items():
        columns.append(sql.SQL('"{}"').format(sql.SQL(field)))
        val = fields_dict.get(field)
        if field_type == "JSONB" and isinstance(val, (dict, list)):
            val = json.dumps(val)
        values.append(val)

    insert_sql = sql.SQL("INSERT INTO {table} ({fields}) VALUES ({placeholders})").format(
        table=sql.Identifier(measurement),
        fields=sql.SQL(", ").join(columns),
        placeholders=sql.SQL(", ").join(sql.Placeholder() * len(columns))
    )
    pg_cursor.execute(insert_sql, values)

pg_conn.commit()
pg_cursor.close()
pg_conn.close()