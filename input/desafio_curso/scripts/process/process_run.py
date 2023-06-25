
# coding: utf-8

# In[16]:


from pyspark.sql import SparkSession, dataframe
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql import functions as f
import os
import re


# In[17]:


spark = SparkSession.builder.master("local[*]")    .enableHiveSupport()    .getOrCreate()


# In[18]:


# dim_clientes, drop duplicate rows by certaincolumns
df_clientes = spark.sql("select Address_Number,Business_Family,Business_Unit,min(Customer) as Customer ,CustomerKey,Division,Line_of_Business,Phone,Region_Code,Search_Type,MIN(Customer_Type) as Customer_Type,MIN(Regional_Sales_Mgr) as Regional_Sales_Mgr from desafio_curso.tbl_clientes where group by Address_Number,Business_Family,Business_Unit,CustomerKey,Division,Line_of_Business,Phone,Region_Code,Search_Type  ")
#df_clientes = spark.sql("select * from desafio_curso.tbl_clientes   ")


# In[20]:


# drop customerkey empty rows
df_vendas = spark.sql("select * from desafio_curso.tbl_vendas where customerkey <> '' ")


# In[21]:


#dim_localidade
df_divisao = spark.sql("select * from desafio_curso.tbl_divisao")
df_regiao = spark.sql("select * from desafio_curso.tbl_regiao")
#df_endereco = spark.sql("select * from desafio_curso.tbl_endereco")
#df_endereco = spark.sql("select Address_Number,Country,State,min(City) as City,min(Customer_Address_1) as Customer_Address_1 from desafio_curso.tbl_endereco group by Address_Number, country,state")
df_endereco = spark.sql("select Address_Number,Country,IF(ltrim(rtrim(State))=='','nao informado',State) as State,IF(ltrim(rtrim(min(City))) =='' ,'nao informado', min(City)) as City,IF(ltrim(rtrim(min(Customer_Address_1))) =='' ,'nao informado', min(Customer_Address_1)) as Customer_Address_1 from desafio_curso.tbl_endereco group by Address_Number, country,state")
#df_endereco.show()SELECT IIF(Column_Name  = '', '#', Column_Name) FROM Table_Name  


# In[23]:


# clean: replace null values with text:nao informado
#df_endereco = df_endereco.fill({"City": " h"})
#df_endereco=df_endereco.fillna({"City": "naoinformado"})
#df_endereco=df_endereco.withColumn("City", \
#       when(col("City")=="" ,None) \
#          .otherwise(col("City"))) \
#when(col(x) != "", col(x)).otherwise(None)
#df_endereco = df_endereco.withColumn("City",when(trim(col('City')) != ' ', col('City')).otherwise('nao informado'))
#df_endereco = df_endereco.withColumn("Customer_Address_1",when(ltrim(rtrim(col('Customer_Address_1'))) != ' ', col('Customer_Address_1')).otherwise('nao informado'))
#df_endereco = df_endereco.withColumn("state",when(ltrim(rtrim(col('state'))) != ' ', col('state')).otherwise('nao informado'))

#df_endereco = df_endereco.na.fill({'City': 'nao infor'})


# In[24]:


df_clientes.createOrReplaceTempView('clientes')
df_vendas.createOrReplaceTempView('vendas')
df_divisao.createOrReplaceTempView('divisao')
df_regiao.createOrReplaceTempView('regiao')
df_endereco.createOrReplaceTempView('endereco')


# In[26]:


#sql = '''
#    select 
#        count(*)        
#    from vendas as v
#    left join 
    
    
 #   '''


# In[27]:


sql = '''
    select
        v.invoice_date,
        v.customerkey,
        v.sales_amount,
        c.customer,
        c.address_number,
        e.customer_address_1,
        r.region_code,
        r.region_name,
        d.division,
        d.division_name,
        e.state,
        e.country,
        e.city
    
    
    
    from vendas  as v 
    inner join clientes  as c
    on v.customerkey == c.customerkey
    left join endereco as e on e.address_number == c.address_number
    left join divisao  d on d.division == c.division
    left join regiao  r on r.region_code == c.region_code
    
'''


# In[28]:


#inner join clientes as c on v.customerkey==c.customerkey
 #    join endereco as e on c.address_number==e.address_number
#left join endereco as e on c.address_number==e.address_number
#left join divisao as d on d.division==c.division
  #  left join regiao as r on r.region_code=c.region_code


# In[29]:


df_stage = spark.sql(sql)


# In[31]:


# Criação da tabela STAGE
df_stage = spark.sql(sql)
df_stage = df_stage.withColumn('Ano', year(to_timestamp(df_stage.invoice_date,"dd/mm/YYYY")))
df_stage = df_stage.withColumn('Mes', month(to_timestamp(df_stage.invoice_date,"dd/mm/YYYY")))
df_stage = df_stage.withColumn('Dia', dayofmonth(to_timestamp(df_stage.invoice_date,"dd/mm/YYYY")))
df_stage = df_stage.withColumn('Trimestre', quarter(to_timestamp(df_stage.invoice_date,"dd/mm/YYYY")))

# Criação dos Campos Calendario para dimension tempo
#df_stage = (df_stage
#                .withColumn('Ano', year(df_stage.invoice_date))
#               .withColumn('Mes', month(df_stage.invoice_date))
 #               .withColumn('Dia', dayofmonth(df_stage.invoice_date))
#               .withColumn('Trimestre', quarter(df_stage.invoice_date))
 #          )


# In[32]:


df_stage = df_stage.withColumn("DW_CLIENTES", sha2(concat_ws("", df_stage.customerkey, df_stage.customer), 256)) 
df_stage = df_stage.withColumn("DW_TEMPO", sha2(concat_ws("", df_stage.invoice_date, df_stage.Ano, df_stage.Mes, df_stage.Dia), 256)) 
#df_stage = df_stage.withColumn("DW_LOCALIDADE", sha2(concat_ws("", df_stage.address_number, df_stage.region_code, df_stage.division, df_stage.customer_address_1, df_stage.city, df_stage.country, df_stage.state, df_stage.region_name, df_stage.division_name), 256))
df_stage = df_stage.withColumn("DW_LOCALIDADE", sha2(concat_ws("", df_stage.address_number, df_stage.region_code, df_stage.division, df_stage.region_name, df_stage.division_name), 256))


# In[34]:


df_stage.count()


# In[42]:


df_stage.schema


# In[24]:


#salvar_df(df_stage, 'total_test')


# In[35]:



df_stage.createOrReplaceTempView('stage')


# In[36]:


#Criando a dimensão Clientes
dim_clientes = spark.sql('''
    SELECT DISTINCT
        DW_CLIENTES,
        customerkey,
        customer
     FROM stage    
''')


# In[37]:


#Criando a dimensão Tempo
dim_tempo = spark.sql('''
    SELECT DISTINCT
        DW_TEMPO,
        invoice_date,
        Ano,
        Mes,
        Dia
    FROM stage    
''')


# In[39]:


#Criando a dimensão Localidade
dim_localidade = spark.sql('''
    SELECT DISTINCT
        DW_LOCALIDADE,
        address_number,
        region_code,
        division,
        if(customer_address_1 is null,"nao informado",customer_address_1) as customer_address_1,
        if(city is null,"nao informado",city) as city,
        if(country is null,"nao informado",country) as country,
        if(state is null,"nao informado",state) as state,
        region_name,
        division_name

    FROM stage    
''')


# In[50]:


#test Group by over DF
#df_stage = df_stage.withColumn("sales_amount", df_stage.sales_amount.cast("double"))
#df_stage.schema
#df_stage.groupBy("DW_CLIENTES","DW_LOCALIDADE","DW_TEMPO").sum("sales_amount").show(50, False)


# In[45]:


#Criando a Fato Vendas
ft_vendas = spark.sql('''
    SELECT 
        DW_CLIENTES,
        DW_LOCALIDADE,
        DW_TEMPO,
        sum(sales_amount)  as vltotal
    FROM stage
    group by 
        DW_CLIENTES,
        DW_LOCALIDADE,
        DW_TEMPO
''')


# In[23]:


# função para salvar os dados
def salvar_df(df, file):
    output = "/input/desafio_curso/gold/" + file
    erase = "hdfs dfs -rm " + output + "/*"
    rename = "hdfs dfs -get /datalake/gold/"+file+"/part-* /input/desafio_curso/gold/"+file+".csv"
    print(rename)
    
    
    df.coalesce(1).write        .format("csv")        .option("header", True)        .option("delimiter", ";")        .mode("overwrite")        .save("/datalake/gold/"+file+"/")

    os.system(erase)
    os.system(rename)


# In[30]:


salvar_df(ft_vendas, 'ft_vendas')
salvar_df(dim_clientes, 'dim_clientes')
salvar_df(dim_tempo, 'dim_tempo')
salvar_df(dim_localidade, 'dim_localidade')

