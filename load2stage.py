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

def ingest_in_sales(session)-> None:
    session.sql("copy into sales_dwh.source.in_sales_order from(\
    select\
    in_sales_order_seq.nextval,\
    t.$1::text as order_id,\
    t.$2::text as customer_name,\
    t.$3::text as mobile_key,\
    t.$4::number as order_quantity,\
    t.$5::number as unit_price,\
    t.$6::number as order_value,\
    t.$7::text as promotion_code,\
    t.$8::number(10,2) as final_order_amount,\
    t.$9::number(10,2) as tax_amount,\
    t.$10::date as order_dt,\
    t.$11::text as payment_status,\
    t.$12::text as shipping_status,\
    t.$13::text as payment_method,\
    t.$14::text as payment_provider,\
    t.$15::text as mobile,\
    t.$16::text as shipping_address,metadata$filename as stg_file_name,\
    metadata$file_row_number as stg_row_number,\
    metadata$file_last_modified as stg_last_modified\
    from\
    @my_internal_stg/source=IN/format=csv/date=2020-01-01/order-202001\
    01.csv(\
        file_format=> 'sales_dwh.common.my_csv_format')\
    t)\
    on_error = 'Continue'\
    ;\
    ")

def ingest_fr_sales(session)->None:
    session.sql('COPY INTO sales_dwh.source.fr_sales_order\
    FROM (\
        SELECT\
    sales_dwh.source.fr_sales_order_seq.NEXTVAL AS id,\
    $1:"Order ID"::TEXT AS order_id,\
    $1:"Customer Name"::TEXT AS customer_name,\
    $1:"Mobile Model"::TEXT AS mobile_key,\
    TO_NUMBER($1:"Quantity") AS quantity,\
    TO_NUMBER($1:"Price per Unit") AS unit_price,\
    TO_DECIMAL($1:"Total Price") AS total_price,\
    $1:"Promotion Code"::TEXT AS promotion_code,\
    TO_NUMBER($1:"Order Amount") AS order_amount,\
    TO_DECIMAL($1:"Tax") AS tax,\
    TO_DATE($1:"Order Date") AS order_dt,\
    $1:"Payment Status"::TEXT AS payment_status,\
    $1:"Shipping Status"::TEXT AS shipping_status,\
    $1:"Payment Method"::TEXT AS payment_method,\
    $1:"Payment Provider"::TEXT AS payment_provider,\
    $1:"Phone"::TEXT AS phone,\
    $1:"Delivery Address"::TEXT AS shipping_address,\
    METADATA$FILENAME AS stg_file_name,\
    METADATA$FILE_ROW_NUMBER AS stg_row_number,\
    METADATA$FILE_LAST_MODIFIED AS stg_last_modified\
    FROM @my_internal_stg/source=FR/format=json/date=2020-01-02/order-20200102.json(\
        FILE_FORMAT => "sales_dwh.common.my_json_format"\
    ) t\
    )\
    ON_ERROR = "CONTINUE";\
    ')

def ingest_us_sales(session)->None:
    session.sql("COPY INTO sales_dwh.source.us_sales_order\
    FROM\
        (\
        SELECT\
    us_sales_order_seq.nextval AS ID,\
    $1:'Order ID'::TEXT AS order_id,\
    $1:'Customer Name'::TEXT AS customer_name,\
    $1:'Mobile Model'::TEXT AS mobile_key,\
    TO_NUMBER($1:'Quantity') AS quantity,\
    TO_NUMBER($1:'Price per Unit') AS unit_price,\
    TO_DECIMAL($1:'Total Price') AS total_price,\
    $1:'Promotion Code'::TEXT AS promotion_code,\
    $1:'Order Amount'::NUMBER(10, 2) AS order_amount,\
    TO_DECIMAL($1:'Tax') AS tax,\
    $1:'Order Date'::DATE AS order_date,\
    $1:'Payment Status'::TEXT AS payment_status,\
    $1:'Shipping Status'::TEXT AS shipping_status,\
    $1:'Payment Method'::TEXT AS payment_method,\
    $1:'Payment Provider'::TEXT AS payment_provider,\
    $1:'Phone''::TEXT AS phone,\
    $1:'Delivery Address'::TEXT AS shipping_address,\
    METADATA$FILENAME AS stg_file_name,\
    METADATA$FILE_ROW_NUMBER AS stg_row_number,\
    METADATA$FILE_LAST_MODIFIED AS stg_last_modified\
    FROM\
    @my_internal_stg/source=US/format=parquet/date=2020-01-02/\
                                                   order-20200102.snappy.parquet\
                                                                  (file_format => sales_dwh.common.my_parquet_format)\
    )\
    ON_ERROR = CONTINUE;\
    ")

def main():
    session = get_snowpark_session()

    #ingest in INDIA sales data
    ingest_in_sales(session)

    # ingest in FRANCE sales data
    ingest_fr_sales(session)

    # ingest in USA sales data
    ingest_us_sales(session)

if __name__ == "__main__":
    main()