use dws;
drop table if exists dws.su_order_ext_spark;
CREATE EXTERNAL TABLE dws.su_order_ext_spark(
real_pay_price DECIMAL(20, 2) comment '真实付款金额'
)
comment '订单扩展表'
PARTITIONED BY (partkey string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUET
LOCATION '/hive/db/dws/SU_ORDER_EXT_SPARK';


