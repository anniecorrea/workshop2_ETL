import pandas as pd
import mysql.connector
import os
from datetime import datetime

config = {
    'user': 'root',         
    'password': 'annie',     
    'host': 'localhost'
}

DB_NAME = 'db_grammys'
TABLE_NAME = 'grammys'

# ------------------ CONEXIÓN ------------------ #
try:
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    print("Conectado a MySQL exitosamente.")
except mysql.connector.Error as err:
    print(f"Error de conexión: {err}")
    exit(1)

def create_database(cursor):
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME} DEFAULT CHARACTER SET 'utf8mb4'")
    print(f"Base de datos '{DB_NAME}' creada o ya existente.")

create_database(cursor)
conn.database = DB_NAME

# ------------------ LECTURA DEL CSV ------------------ #
csv_path = os.path.join(os.path.dirname(__file__), "data\\the_grammy_awards.csv")

try:
    df = pd.read_csv(csv_path)
    print(f"Archivo CSV cargado: {csv_path}")
except FileNotFoundError:
    print("No se encontró el archivo CSV. Verifica la ruta.")
    exit(1)

df.columns = [col.strip().replace(" ", "").replace("-", "").replace("/", "_") for col in df.columns]

# ------------------ DETECCIÓN DE TIPOS ------------------ #
def detect_mysql_type(series: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(series.dropna()):
        return "INT"
    elif pd.api.types.is_float_dtype(series.dropna()):
        return "FLOAT"
    elif pd.api.types.is_bool_dtype(series.dropna()):
        return "TINYINT(1)"  # Boolean en MySQL
    elif pd.api.types.is_datetime64_any_dtype(series):
        return "DATETIME"
    else:
        sample = series.dropna().astype(str).head(10)
        date_like = 0
        for val in sample:
            try:
                datetime.strptime(val, "%Y-%m-%d")
                date_like += 1
            except:
                pass
        if date_like >= len(sample) / 2:
            return "DATE"
        max_len = series.astype(str).map(len).max() if not series.empty else 0
        return "VARCHAR(255)" if max_len < 255 else "TEXT"

print("\nTipos detectados por columna:")
columns_sql_parts = []
for col in df.columns:
    sql_type = detect_mysql_type(df[col])
    columns_sql_parts.append(f"{col} {sql_type}")
    print(f" - {col}: {sql_type}")
columns_sql = ",\n    ".join(columns_sql_parts)

create_table_query = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    id INT AUTO_INCREMENT PRIMARY KEY,
    {columns_sql}
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

cursor.execute(create_table_query)
print(f"\nTabla '{TABLE_NAME}' creada correctamente con tipos detectados automáticamente.")

# Convertir valores booleanos y similares a 0/1
def to_mysql_compatible(value):
    if pd.isna(value):
        return None
    if isinstance(value, bool):
        return int(value)
    if str(value).lower() in ['true', 'yes', 'y', '1']:
        return 1
    if str(value).lower() in ['false', 'no', 'n', '0']:
        return 0
    return str(value)

inserted = 0
for _, row in df.iterrows():
    values = tuple(to_mysql_compatible(x) for x in row)
    placeholders = ", ".join(["%s"] * len(row))
    insert_query = f"INSERT INTO {TABLE_NAME} ({', '.join(df.columns)}) VALUES ({placeholders})"
    cursor.execute(insert_query, values)
    inserted += 1

conn.commit()
print(f"\n{inserted} registros insertados en la tabla '{TABLE_NAME}' correctamente.")

cursor.close()
conn.close()
print("Conexión cerrada correctamente.")