CREATE EXTERNAL TABLE IF NOT EXISTS desafio_curso.REGIAO ( 
        Region_Code string,
	    Region_Name string
    )
COMMENT 'tabela de REGIAO'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/REGIAO/'
TBLPROPERTIES ("skip.header.line.count"="1");


CREATE TABLE desafio_curso.tbl_REGIAO ( 
        Region_Code string,
	    Region_Name string
    )
PARTITIONED BY (DT_FOTO STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
TBLPROPERTIES ('ORC.COMPRESS'='SNAPPY');

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE
    desafio_curso.tbl_REGIAO
PARTITION(DT_FOTO)
SELECT
Region_Code string,
Region_Name string,
'22062023' as DT_FOTO
FROM desafio_curso.REGIAO;