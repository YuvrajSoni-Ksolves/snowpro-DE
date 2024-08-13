from snowflake.snowpark import Session

connection_param = {
    "account" : "ljmdnnl-tw27020",
    "user" : "yuvraj2172",
    "password" : "Yuvrajsoni123456"
}

session = Session.builder.configs(connection_param).create()

call_center_df = session.table("SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CALL_CENTER")

call_center_df.show(10)

database = "SNOWFLAKE_SAMPLE_DATA"
schema = "TPCDS_SF100TCL"
tablename = "DATE_DIM"

date_dim_df = session.table([database,schema, tablename])
date_dim_df.show(20)