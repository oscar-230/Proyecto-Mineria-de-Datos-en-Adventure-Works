# -----------------------------------------
# Avance #3 Proyecto Final ETL
# Oscar David Cuaical López
# 202270657
# cuaical.oscar@correounivalle.edu.co
# -----------------------------------------

import pandas as pd
from sqlalchemy import create_engine

# -----------------------------------------
# 1. CONEXIONES A LAS BASES DE DATOS
# -----------------------------------------

# SQL Server origen
SQLSERVER_CONNECTION_STRING = (
    "mssql+pyodbc://@localhost/AdventureWorks2022"
    "?driver=ODBC+Driver+17+for+SQL+Server"
    "&Trusted_Connection=yes"
)
engine_sql = create_engine(SQLSERVER_CONNECTION_STRING)

# PostgreSQL destino
PG_CONNECTION_STRING = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
PG_CONF = {
    "user": "postgres",
    "password": "admin",
    "host": "localhost",
    "port": 5432,
    "database": "dw_ventas_adventureworks2022"
}
engine_pg = create_engine(PG_CONNECTION_STRING.format(**PG_CONF))

# Schema destino
PG_SCHEMA = "dm_ventas"


# -----------------------------------------
# 2. EXTRAER TABLA DESDE SQL SERVER
# -----------------------------------------
def extraer_tabla(nombre_tabla):
    """Lee una tabla desde SQL Server y retorna un DataFrame."""
    query = f"SELECT * FROM {nombre_tabla};"
    return pd.read_sql(query, engine_sql)


# -----------------------------------------
# 3. CARGAR TABLA EN POSTGRESQL
# -----------------------------------------
def cargar_en_postgres(df, nombre_tabla):
    """Carga un DataFrame en PostgreSQL dentro del schema destino."""
    df.to_sql(
        nombre_tabla,
        engine_pg,
        schema=PG_SCHEMA,
        if_exists="replace",
        index=False
    )
    print(f"Tabla '{nombre_tabla}' cargada correctamente.")


# -----------------------------------------
# 4. PROCESO ETL
# -----------------------------------------
def ejecutar_etl():
    tablas_origen = [
        "Person.Person",
        "Production.Product",
        "Sales.SalesOrderHeader",
        "Sales.SalesOrderDetail"
    ]

    for tabla in tablas_origen:
        print(f"\nExtrayendo: {tabla}")
        df = extraer_tabla(tabla)

        nombre_destino = tabla.split(".")[1]

        print(f"Cargando: {nombre_destino}")
        cargar_en_postgres(df, nombre_destino)

    print("\nETL FINALIZADO CORRECTAMENTE.")


# -----------------------------------------
# 5. EJECUCIÓN
# -----------------------------------------
if __name__ == "__main__":
    ejecutar_etl()
