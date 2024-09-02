import sys

from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.types import StructType, StringType, StructField, StringType,LongType,DecimalType,DateType,TimestampType
from snowflake.snowpark.functions import col,lit,row_number, rank
from snowflake.snowpark import Window

def get_snowpark_session() -> Session:
    connection_parameters = {
        "ACCOUNT":"ljmdnnl-tw27020",
        "USER":"yuvraj2172",
        "PASSWORD":"Yuvrajsoni123456",
        "ROLE":"accountadmin",
        "DATABASE":"SNOWFLAKE_SAMPLE_DATA",
        "SCHEMA":"TPCH_SF1",
        "WAREHOUSE":"SNOWPARK_ETL_WH"
    }
    # creating snowflake session object
    return Session.builder.configs(connection_parameters).create()