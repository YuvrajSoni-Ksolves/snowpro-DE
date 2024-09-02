from snowflake.snowpark import Session
import sys
import logging

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

def main():
    session = get_snowpark_session()

    context_df = session.sql("select current_role(), current_database(), current_schema(), current_warehouse()")
    context_df.show(2)

    customer_df = session.sql("select c_custkey,c_name,c_phone,c_mktsegment from snowflake_sample_data.tpch_sf1.customer limit 10")
    customer_df.show(5)

if __name__ == '__main__':
    main()