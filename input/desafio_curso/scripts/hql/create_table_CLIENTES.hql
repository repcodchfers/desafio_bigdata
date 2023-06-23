CREATE EXTERNAL TABLE IF NOT EXISTS desafio_curso.CLIENTES ( 
        Address_Number string, 
	    Business_Family string,
	    Business_Unit string, 
	    Customer string,
	    CustomerKey string,
	    Customer_Type string,
	    Division string,
	    Line_of_Business string,
	    Phone string,
	    Region_Code string,
	    Regional_Sales_Mgr string,
	    Search_Type string
    )
COMMENT 'tabela de Clientes'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/CLIENTES/'
TBLPROPERTIES ("skip.header.line.count"="1");


CREATE TABLE desafio_curso.tbl_CLIENTES ( 
        Address_Number string, 
	    Business_Family string,
	    Business_Unit string, 
	    Customer string,
	    CustomerKey string,
	    Customer_Type string,
	    Division string,
	    Line_of_Business string,
	    Phone string,
	    Region_Code string,
	    Regional_Sales_Mgr string,
	    Search_Type string
    )
PARTITIONED BY (DT_FOTO STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
TBLPROPERTIES ('ORC.COMPRESS'='SNAPPY');

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE
    desafio_curso.tbl_CLIENTES
PARTITION(DT_FOTO)
SELECT
Address_Number string, 
Business_Family string,
Business_Unit string, 
Customer string,
CustomerKey string,
Customer_Type string,
Division string,
Line_of_Business string,
Phone string,
Region_Code string,
Regional_Sales_Mgr string,
Search_Type string,
'22062023' as DT_FOTO
FROM desafio_curso.CLIENTES;