# 📊 Proyecto: Minería de Datos en Adventure Works

## 🏫 Información General

- **Universidad:** Universidad del Valle  
- **Facultad:** Escuela de Ingeniería de Sistemas y Computación  
- **Asignatura:** Introducción a la Ciencia de Datos  
- **Docente:** Susana Medina  
- **Semestre:** 2025-II  

## 👤 Autor

- **Nombre:** Oscar David Cuaical López  
- **Código:** 202270657  
- **Correo:** cuaical.oscar@correounivalle.edu.co  

---

## 📌 Descripción del Proyecto

Este proyecto tiene como objetivo aplicar técnicas de **ingeniería de datos y minería de datos** utilizando la base de datos **AdventureWorks 2022**, un modelo transaccional ampliamente utilizado con fines académicos.

A lo largo del proyecto se desarrolló un proceso completo que abarca:

- Análisis del modelo relacional (OLTP)
- Diseño de la bodega de datos (OLAP)
- Migración entre sistemas gestores de bases de datos
- Construcción de un proceso ETL
- Desarrollo de visualizaciones para análisis de negocio

El enfoque principal fue transformar datos operacionales en información útil para la **toma de decisiones empresariales**, especialmente en el área de ventas.

---

## ⚙️ Actividades Realizadas

### 1. Modelo Relacional

Se analizó la base de datos transaccional **AdventureWorks 2022**, compuesta por múltiples áreas de negocio como ventas, compras, producción, recursos humanos y clientes.

- Se generó el **esquema relacional** desde SQL Server.
- Se construyó un **Modelo Entidad-Relación (MER)** en notación UML.
- El modelado se realizó de forma **modular** para facilitar la comprensión.

Este análisis permitió entender la estructura base para el diseño del Data Warehouse.

---

### 2. Modelo Dimensional (Data Warehouse)

Se diseñó una **bodega de datos** bajo un esquema en estrella (Star Schema), optimizado para consultas analíticas.

Se definieron los siguientes **datamarts**:

- Ventas  
- Compras  
- Producción e Inventarios  
- Recursos Humanos  
- Clientes y Personas  

Cada datamart incluye:

- **Tablas de hechos:** eventos medibles del negocio  
- **Dimensiones:** contexto descriptivo (tiempo, producto, cliente, etc.)  

Esto permite analizar el negocio desde múltiples perspectivas como tiempo, ubicación y tipo de cliente.

---

### 3. Migración de SQL Server a PostgreSQL

Se realizó la migración de la bodega de datos desde **SQL Server** hacia **PostgreSQL**.

Aspectos clave:

- Uso de herramientas como **ESF Database Migration Toolkit**
- Conversión de tipos de datos (ej: `DATETIME → TIMESTAMP`)
- Adaptación de claves primarias y foráneas
- Verificación de integridad de datos

La migración permitió trasladar completamente la estructura y la información al nuevo entorno sin afectar el modelo conceptual.

---

### 4. Proceso ETL (Extract, Transform, Load)

Se implementó un proceso ETL en **Python** utilizando:

- `pandas`
- `SQLAlchemy`

#### 🔹 Etapas del ETL:

1. **Extracción:** datos desde SQL Server  
2. **Carga inicial:** almacenamiento en PostgreSQL (staging)  
3. **Transformación:** creación del Data Mart de ventas  

#### 🔹 Dimensiones construidas:

- `dim_customer`
- `dim_employee`
- `dim_product`
- `dim_reseller`
- `dim_sales_territory`
- `dim_ship_method`
- `dim_date`

#### 🔹 Tablas de hechos:

- `fact_internet_sales`
- `fact_reseller_sales`

#### 🔹 Medidas principales:

- Cantidad vendida  
- Precio unitario  
- Descuentos  
- Total por línea  
- Impuestos y costos asociados  

#### 🔹 Granularidad:

El nivel de detalle es **una fila por cada línea de producto vendida** (SalesOrderDetail).

---

### 5. Visualización de Datos

Se desarrolló un módulo de visualización utilizando **Power BI**, donde se construyeron dashboards para:

- Ventas por Internet  
- Ventas por Revendedores  

Los reportes permiten analizar:

- Ingresos
- Tendencias de ventas
- Desempeño por producto
- Comportamiento por región
---

## 🔄 Flujo General del Proyecto

1. Análisis del modelo relacional (OLTP)  
2. Diseño del modelo dimensional (OLAP)  
3. Migración de datos a PostgreSQL  
4. Construcción del proceso ETL  
5. Creación del Data Mart de ventas  
6. Visualización y análisis de datos  

---

## 🎯 Conclusiones

- Se logró transformar una base de datos transaccional en un modelo analítico eficiente.  
- El diseño dimensional facilita consultas complejas y análisis multidimensional.  
- El proceso ETL automatiza la integración y transformación de datos.  
- Las visualizaciones permiten interpretar los datos de forma clara y apoyar la toma de decisiones.  

---

## 📁 Recursos Adicionales

- 📊 Archivo Power BI (.pbix) incluido en el repositorio  
- 🎥 Video ETL: https://www.youtube.com/watch?v=kgu0uMpmfcA

---
