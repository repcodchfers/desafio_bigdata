#!/bin/bash

TABLES=("CLIENTES")
for i in "${TABLES[@]}"
do
    echo "tabela $i"
    cd ../../raw/
    hdfs dfs -mkdir /datalake/raw/$i
    hdfs dfs -chmod 777 /datalake/raw/$i
    hdfs dfs -copyFromLocal $i.csv /datalake/raw/$i
    beeline -u jdbc:hive2://localhost:10000 -f ../../scripts/hql/create_table_$i.hql 
done

#TABLES=("CLIENTES" "DIVISAO" "ENDERECO" "REGIAO" "VENDAS")
#test for clientes table
# Criação das pastas

#DADOS=("cidade" "estado" "filial" "parceiro" "cliente" "subcategoria" "categoria" "item_pedido" "produto")
#
#for i in "${DADOS[@]}"
#do
#	echo "$i"
#    cd ../../raw/
#    hdfs dfs -mkdir /datalake/raw/$i
#    hdfs dfs -chmod 777 /datalake/raw/$i
#    hdfs dfs -copyFromLocal $i.csv /datalake/raw/$i
#    beeline -u jdbc:hive2://localhost:10000 -f ../../scripts/hql/create_table_$i.hql 
#done
