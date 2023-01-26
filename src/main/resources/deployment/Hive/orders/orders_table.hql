create database if not exists jea_db;
use jea_db;
create external table if not exists orders (
  orderid int,
  customerid string,
  campaignid int,
  orderdate string,
  city string,
  state string,
  zipcode string,
  paymenttype string,
  totalprice float,
  numorderlines int,
  numunits int
)
row format delimited
fields terminated by ';'
lines terminated by '\n'
stored as textfile location 'hdfs://namenode:8020/user/hive/warehouse/jea_db.db/orders';