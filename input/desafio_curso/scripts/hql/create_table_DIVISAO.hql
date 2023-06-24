CREATE EXTERNAL TABLE IF NOT EXISTS desafio_curso.DIVISAO ( 
        Division string,
	    Division_Name string
    )
COMMENT 'tabela de DIVISAO'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/DIVISAO/'
TBLPROPERTIES ("skip.header.line.count"="1");


CREATE TABLE desafio_curso.tbl_DIVISAO ( 
        Division string,
	    Division_Name string
    )
PARTITIONED BY (DT_FOTO STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
TBLPROPERTIES ('ORC.COMPRESS'='SNAPPY');

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE
    desafio_curso.tbl_DIVISAO
PARTITION(DT_FOTO)
SELECT
Division string,
Division_Name string,
'22062023' as DT_FOTO
FROM desafio_curso.DIVISAO;