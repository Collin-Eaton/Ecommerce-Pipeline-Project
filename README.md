Data is generated through a simulated API built with FastAPI. A Python ETL pipeline extracts that data and loads it into Snowflake raw tables. From there, dbt transforms the data into staging models for cleaning and validation, and then into marts models using a star schema for analytics consumption
 
 +----------------------+
                    |   API Simulator      |
                    | (FastAPI + Faker)   |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |   Python ETL Layer   |
                    |  (Extract/Transform) |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |     Snowflake        |
                    |   Raw Tables (RAW)   |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |        dbt           |
                    |  Staging Models      |
                    |  (stg_orders, etc)   |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |      dbt Marts       |
                    | Fact & Dimension     |
                    | (fct_orders, dim_*)  |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |   Analytics Layer    |
                    | (BI / Reporting)     |
                    +----------------------+
