import sys
from distutils.command.install_egg_info import safe_name

from snowflake.snowpark import Session, DataFrame
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

def filter_dataset(df, column_name, filter_criterian)-> DataFrame:
    return_df = df.filter(col(column_name) == filter_criterian)

    return return_df

def main():
    session = get_snowpark_session()


    sales_df = session.sql("select * from sales_dwh.source.in_sales_order")
    sales_df.show()

    # apply filter to select only paid and delivered sale orders
    paid_sales_df = filter_dataset(sales_df,"PAYMENT_STATUS", "Paid")
    # paid_sales_df.show()

    shipped_sales_df = filter_dataset(paid_sales_df, "SHIPPING_STATUS", "Delivered")
    # shipped_sales_df.show()

    # adding country and region column to the dataframe
    country_sales_df = shipped_sales_df.withColumn('Country', lit('IN')).withColumn("Region", lit("APAC"))
    # country_sales_df.show()

    forex_df = session.sql("select * from sales_dwh.common.exchange_rate")
    # forex_df.show()
    # join to add forex calculations
    sales_with_forex_df = country_sales_df.join(forex_df,country_sales_df['order_dt'] == forex_df['date'], join_type='outer')
    # sales_with_forex_df.show()

    unique_orders = sales_with_forex_df.withColumn('order_rank', rank().over(Window.partitionBy(col('order_dt')).order_by(col('_metadata_last_modified').desc()))).filter(col('order_rank') == 1).select(col('sales_order_key').alias('unique_sales_order_key'))
    # unique_orders.show()

    final_sales_df = unique_orders.join(sales_with_forex_df, unique_orders['unique_sales_order_key'] == sales_with_forex_df['sales_order_key'], join_type='inner')
    # final_sales_df.show()

    final_sales_df = final_sales_df.select(
        col('SALES_ORDER_KEY'),
        col('ORDER_ID'),
        col('ORDER_DT'),
        col('CUSTOMER_NAME'),
        col('MOBILE_KEY'),
        col('Country'),
        col('Region'),
        col('ORDER_QUANTITY'),
        lit('INR').alias('LOCAL_CURRENCY'),
        col('UNIT_PRICE').alias('LOCAL_UNIT_PRICE'),
        col('PROMOTION_CODE').alias('PROMOTION_CODE'),
        col('FINAL_ORDER_AMOUNT').alias('LOCAL_TOTAL_ORDER_AMT'),
        col('TAX_AMOUNT').alias('local_tax_amt'),
        col('USD2INR').alias("Exhchange_Rate"),
        (col('FINAL_ORDER_AMOUNT')/col('USD2INR')).alias('US_TOTAL_ORDER_AMT'),
        (col('TAX_AMOUNT')/col('USD2INR')).alias('USD_TAX_AMT'),
        col('payment_status'),
        col('shipping_status'),
        col('payment_method'),
        col('payment_provider'),
        col('mobile').alias('conctact_no'),
        col('shipping_address')
    )

    # final_sales_df.show()
    final_sales_df.write.saveAsTable("sales_dwh.curated.in_sales_order", mode= "append")

if __name__=="__main__":
    main()