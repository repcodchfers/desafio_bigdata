CREATE EXTERNAL TABLE IF NOT EXISTS desafio_curso.ENDERECO ( 
        Address_Number string,
	    City string,
	    Country string,
	    Customer_Address_1 string,
	    Customer_Address_2 string,
	    Customer_Address_3 string,
	    Customer_Address_4 string,
	    State string,
	    Zip_Code string
    )
COMMENT 'tabela de ENDERECO'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/ENDERECO/'
TBLPROPERTIES ("skip.header.line.count"="1");


CREATE TABLE desafio_curso.tbl_ENDERECO ( 
        Address_Number string,
	    City string,
	    Country string,
	    Customer_Address_1 string,
	    Customer_Address_2 string,
	    Customer_Address_3 string,
	    Customer_Address_4 string,
	    State string,
	    Zip_Code string
    )
PARTITIONED BY (DT_FOTO STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
TBLPROPERTIES ('ORC.COMPRESS'='SNAPPY');

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE
    desafio_curso.tbl_ENDERECO
PARTITION(DT_FOTO)
SELECT
Address_Number string,
City string,
Country string,
Customer_Address_1 string,
Customer_Address_2 string,
Customer_Address_3 string,
Customer_Address_4 string,
State string,
Zip_Code string,
'22062023' as DT_FOTO
FROM desafio_curso.ENDERECO;