# desafio_bigdata
DESAFIO BIG DATA/MODELAGEM

O objetivo do desafio é realizar a ingestão de uma fonte de dados raw .csv (banco de dados relacional de vendas) para a estrutura hdfs datalake/gold, realizar o processamento e limpeza dos dados das tabelas hive usando o pyspark e então levar os dados num modelo star schema e ser consumido em seguida, por uma ferramenta de visualização para criar relatórios e dashboards 

ETAPAS
Etapa 1 - Enviar os arquivos para o HDFS
    - por meio de um shell script realizou-se a copia desde a pasta de dados raw (vendas.csv;clientes.csv;endereço.csv;região.csv;divisão.csv) para a estrutura hdfs /datalake/gold

Etapa 2 – Por médio de um script executar sentenças Hive . hql(scripts/pre_process) para criar o banco de dados desafio_curso e as tabelas: 

        - TBL_VENDAS
        - TBL_CLIENTES
        - TBL_ENDERECO
        - TBL_REGIAO
        - TBL_DIVISAO

Etapa 3 - Processar os dados no Spark Efetuando suas devidas transformações criando os arquivos com a modelagem de BI por meio do script python desafio_curso/scripts/process/process1.py
Nessa etapa são aplicadas transformações utilizando dataframe spark e usando sentenças Sql (remover linhas duplicadas e com valores nulos; substituir  valores nulos em campos com o texto “não informado”)
 
![diagstar2](https://github.com/repcodchfers/desafio_bigdata/assets/86985900/103ebf00-e29d-4a74-8c74-138da5ec568d)

Etapa 4 - Gravar as informações em tabelas dimensionais em formato cvs delimitado por ';'.

        - FT_VENDAS
        - DIM_CLIENTES
        - DIM_TEMPO
        - DIM_LOCALIDADE

Etapa 5 - Exportar os dados para a pasta desafio_curso/gold

Etapa 6 – Criação do relatório com indicadores e gráficos de vendas
•	Vendas por cliente
•	Vendas por ano
•	Vendas por estado
•	Valor total de vendas
