CREATE EXTERNAL TABLE IF NOT EXISTS desafio_curso.VENDAS ( 
        Actual_Delivery_Date string,
	    CustomerKey string,
	    DateKey string,
	    Discount_Amount string,
	    Invoice_Date string,
	    Invoice_Number string,
	    Item_Class string,
	    Item_Number string,
	    Item string,
	    Line_Number string,
	    List_Price string,
	    Order_Number string,
	    Promised_Delivery_Date string,
	    Sales_Amount string,
	    Sales_Amount_Based_on_List_Price string,
	    Sales_Cost_Amount string,
	    Sales_Margin_Amount string,
	    Sales_Price string,
	    Sales_Quantity string,
	    Sales_Rep string,
	    U_M string
    )
COMMENT 'tabela de VENDAS'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/VENDAS/'
TBLPROPERTIES ("skip.header.line.count"="1");


CREATE TABLE desafio_curso.tbl_VENDAS ( 
        Actual_Delivery_Date string,
	    CustomerKey string,
	    DateKey string,
	    Discount_Amount string,
	    Invoice_Date string,
	    Invoice_Number string,
	    Item_Class string,
	    Item_Number string,
	    Item string,
	    Line_Number string,
	    List_Price string,
	    Order_Number string,
	    Promised_Delivery_Date string,
	    Sales_Amount string,
	    Sales_Amount_Based_on_List_Price string,
	    Sales_Cost_Amount string,
	    Sales_Margin_Amount string,
	    Sales_Price string,
	    Sales_Quantity string,
	    Sales_Rep string,
	    U_M string
    )
PARTITIONED BY (DT_FOTO STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
TBLPROPERTIES ('ORC.COMPRESS'='SNAPPY');

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE
    desafio_curso.tbl_VENDAS
PARTITION(DT_FOTO)
SELECT
Actual_Delivery_Date string,
CustomerKey string,
DateKey string,
Discount_Amount string,
Invoice_Date string,
Invoice_Number string,
Item_Class string,
Item_Number string,
Item string,
Line_Number string,
List_Price string,
Order_Number string,
Promised_Delivery_Date string,
Sales_Amount string,
Sales_Amount_Based_on_List_Price string,
Sales_Cost_Amount string,
Sales_Margin_Amount string,
Sales_Price string,
Sales_Quantity string,
Sales_Rep string,
U_M string,
'22062023' as DT_FOTO
FROM desafio_curso.VENDAS;