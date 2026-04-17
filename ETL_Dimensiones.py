# -----------------------------------------
# Avance #3 Proyecto Final ETL
# Oscar David Cuaical López
# 202270657
# cuaical.oscar@correounivalle.edu.co
# -----------------------------------------

import pandas as pd
from sqlalchemy import create_engine

# ======================================================
# 1. CONEXIONES A SQL SERVER Y POSTGRES
# ======================================================

SQLSERVER_CONNECTION_STRING = (
    "mssql+pyodbc://@localhost/AdventureWorks2022"
    "?driver=ODBC+Driver+17+for+SQL+Server"
    "&Trusted_Connection=yes"
)

PG_CONNECTION_STRING = "postgresql+psycopg2://postgres:admin@localhost:5432/dw_ventas_adventureworks2022"
PG_SCHEMA = "dm_ventas"

engine_sql = create_engine(SQLSERVER_CONNECTION_STRING)
engine_pg = create_engine(PG_CONNECTION_STRING)


# ======================================================
# 2. FUNCIONES GENERALES
# ======================================================

def leer(tabla):
    """Lee una tabla desde dm_ventas en PostgreSQL."""
    return pd.read_sql(f'SELECT * FROM {PG_SCHEMA}."{tabla}"', engine_pg)


def guardar(df, tabla):
    """Guarda DataFrame en PostgreSQL."""
    df.to_sql(tabla, engine_pg, schema=PG_SCHEMA, if_exists="replace", index=False)
    print(f"Tabla {tabla} generada correctamente.")


# ======================================================
# 3. CONSTRUIR DIMENSIONES
# ======================================================

def construir_dim_customer():
    df = leer("Person")
    dim = df[[
        "BusinessEntityID",
        "FirstName",
        "MiddleName",
        "LastName",
        "PersonType",
        "EmailPromotion",
        "ModifiedDate"
    ]].copy()

    dim.rename(columns={
        "BusinessEntityID": "CustomerKey"
    }, inplace=True)

    dim["FullName"] = dim["FirstName"] + " " + dim["LastName"]
    guardar(dim, "dim_customer")


def construir_dim_employee():
    df = leer("Person")[["BusinessEntityID", "FirstName", "LastName", "PersonType"]].copy()
    dim = df[df["PersonType"] == "SP"].copy()   # Solo vendedores (Sales Person)
    dim.rename(columns={"BusinessEntityID": "EmployeeKey"}, inplace=True)
    dim["FullName"] = dim["FirstName"] + " " + dim["LastName"]
    guardar(dim, "dim_employee")


def construir_dim_product():
    df = leer("Product")
    dim = df[[
        "ProductID",
        "Name",
        "ProductNumber",
        "Color",
        "StandardCost",
        "ListPrice",
        "ProductSubcategoryID",
        "ProductModelID"
    ]].copy()
    dim.rename(columns={"ProductID": "ProductKey"}, inplace=True)
    guardar(dim, "dim_product")


def construir_dim_ship_method():
    df = pd.read_sql("SELECT ShipMethodID, Name, ShipBase, ShipRate FROM Purchasing.ShipMethod", engine_sql)
    df.rename(columns={"ShipMethodID": "ShipMethodKey"}, inplace=True)
    guardar(df, "dim_ship_method")


def construir_dim_date():
    df = leer("SalesOrderHeader")[["OrderDate"]].copy()
    df["OrderDate"] = pd.to_datetime(df["OrderDate"])
    dim = pd.DataFrame({
        "DateKey": df["OrderDate"].dt.strftime("%Y%m%d").astype(int),
        "Date": df["OrderDate"],
        "Year": df["OrderDate"].dt.year,
        "Month": df["OrderDate"].dt.month,
        "Day": df["OrderDate"].dt.day
    }).drop_duplicates()
    guardar(dim, "dim_date")


def construir_dim_reseller():
    df = leer("SalesOrderHeader")
    dim = df[["TerritoryID"]].drop_duplicates().copy()
    dim.rename(columns={"TerritoryID": "ResellerKey"}, inplace=True)
    dim["ResellerName"] = "Reseller " + dim["ResellerKey"].astype(str)
    guardar(dim, "dim_reseller")


def construir_dim_sales_territory():
    df = leer("SalesOrderHeader")
    dim = df[["TerritoryID"]].drop_duplicates().copy()
    dim.rename(columns={"TerritoryID": "SalesTerritoryKey"}, inplace=True)
    guardar(dim, "dim_sales_territory")


# ======================================================
# 4. TABLAS DE HECHOS
# ======================================================

def construir_fact_internet_sales():
    header = leer("SalesOrderHeader")
    detail = leer("SalesOrderDetail")

    # Solo ventas en línea
    header_online = header[header["OnlineOrderFlag"] == True]

    fact = detail.merge(
        header_online,
        on="SalesOrderID",
        how="inner"
    )

    fact_final = fact[[
        "SalesOrderID",
        "SalesOrderDetailID",
        "CustomerID",
        "SalesPersonID",
        "TerritoryID",
        "ProductID",
        "OrderQty",
        "UnitPrice",
        "UnitPriceDiscount",
        "LineTotal",
        "OrderDate"
    ]].copy()

    fact_final.rename(columns={
        "CustomerID": "CustomerKey",
        "SalesPersonID": "EmployeeKey",
        "TerritoryID": "SalesTerritoryKey",
        "ProductID": "ProductKey"
    }, inplace=True)

    fact_final["DateKey"] = pd.to_datetime(fact_final["OrderDate"]).dt.strftime("%Y%m%d").astype(int)
    fact_final.drop(columns=["OrderDate"], inplace=True)

    guardar(fact_final, "fact_internet_sales")


def construir_fact_reseller_sales():
    header = leer("SalesOrderHeader")
    detail = leer("SalesOrderDetail")

    header_res = header[header["OnlineOrderFlag"] == False]

    fact = detail.merge(
        header_res,
        on="SalesOrderID",
        how="inner"
    )

    fact_final = fact[[
        "SalesOrderID",
        "SalesOrderDetailID",
        "CustomerID",
        "TerritoryID",
        "ProductID",
        "OrderQty",
        "UnitPrice",
        "UnitPriceDiscount",
        "LineTotal",
        "OrderDate"
    ]].copy()

    fact_final.rename(columns={
        "CustomerID": "ResellerKey",
        "TerritoryID": "SalesTerritoryKey",
        "ProductID": "ProductKey"
    }, inplace=True)

    fact_final["DateKey"] = pd.to_datetime(fact_final["OrderDate"]).dt.strftime("%Y%m%d").astype(int)
    fact_final.drop(columns=["OrderDate"], inplace=True)

    guardar(fact_final, "fact_reseller_sales")


# ======================================================
# 5. EJECUCIÓN GENERAL
# ======================================================

def ejecutar_etl_completo():
    print("\n=== GENERANDO DIMENSIONES ===")
    construir_dim_customer()
    construir_dim_employee()
    construir_dim_product()
    construir_dim_ship_method()
    construir_dim_date()
    construir_dim_reseller()
    construir_dim_sales_territory()

    print("\n=== GENERANDO TABLAS DE HECHOS ===")
    construir_fact_internet_sales()
    construir_fact_reseller_sales()

    print("\nETL COMPLETO FINALIZADO EXITOSAMENTE")


if __name__ == "__main__":
    ejecutar_etl_completo()
