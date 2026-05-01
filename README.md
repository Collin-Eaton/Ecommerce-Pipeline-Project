Data is generated through a simulated API built with FastAPI. A Python ETL pipeline extracts that data and loads it into Snowflake raw tables. From there, dbt transforms the data into staging models for cleaning and validation, and then into marts models using a star schema for analytics consumption
 
 [ API Simulator ]
        |
        v
[ Python ETL ]
 (Incremental Load + Logging)
        |
        v
[ Snowflake RAW Layer ]
        |
        v
[ dbt Staging Layer ]
 (Data Cleaning + Tests)
        |
        v
[ dbt Marts Layer ]
 (Star Schema: Fact + Dimensions)
        |
        v
[ Analytics / BI ]
