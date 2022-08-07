import pyspark

from pyspark.sql import SparkSession

spark=SparkSession.builder.appName('spark_scd_type_1').getOrCreate()

emp_src=spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/swapnil").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "emp_spark").option("user", "root").option("password", "root").load()



emp_src.show()

emp_target=spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/swapnil").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "emp_spark_scd1").option("user", "root").option("password", "root").load()



emp_src=emp_src.withColumnRenamed('empno','empno_src').withColumnRenamed('empname','empname_src').withColumnRenamed('deptno','deptno_src').withColumnRenamed('sal','sal_src')



emp_target=emp_target.withColumnRenamed('empno','empno_target').withColumnRenamed('sal','sal_target')



emp_scd=emp_src.join(emp_target,emp_src.empno_src==emp_target.empno_target,'left')

emp_scd.show()

from pyspark.sql.functions import lit

from pyspark.sql import functions as f

scd_df=emp_scd.withColumn('INS_FLAG',f.when((emp_scd.empno_src!=emp_scd.empno_target)| emp_scd.empno_target.isNull(),'Y').otherwise('NA'))

scd_df.show()

emp_ins=scd_df.select(scd_df['empno_src'].alias('empno'),scd_df['empname_src'].alias('empname'),scd_df['deptno_src'].alias('deptno'),scd_df['sal_src'].alias('sal'))



emp_ins.write.format("jdbc").mode('append').option("url", "jdbc:mysql://localhost:3306/swapnil").option("driver","com.mysql.jdbc.Driver").option("dbtable", "emp_spark_scd1").option("user", "root").option("password", "root").save()



#do some insert and update opeations with table record

#insert into emp_spark values(105,'krishna patil',30,22000)

#Update emp_spark set sal=60000 where empno=101

emp_src1=spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/swapnil").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "emp_spark").option("user", "root").option("password", "root").load()
emp_src1.show()

emp_target1=spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/swapnil").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "emp_spark_scd1").option("user", "root").option("password", "root").load()

from pyspark.sql.functions import round, col
emp_target1=emp_target1.select(round('empno',0).alias('empno_tgt'),emp_target1['empname'].alias('empname_tgt'),round('deptno',0).alias('deptno_tgt'),round('sal',2).alias('sal_tgt'))

emp_scd1=emp_src1.join(emp_target1,emp_src1.empno==emp_target1.empno_tgt,'left')
emp_scd1.show()

#insert flag

scd1_df=emp_scd1.withColumn('INS_FLAG',f.when((emp_scd1.empno!=emp_scd1.empno_tgt) | emp_scd1.empno_tgt.isNull(),'Y').otherwise('NA'))
scd1_df.show()

#update flag

scd2_df=scd1.df.withColumn('UPD_FLAG',f.when((scd1_df.empno==scd1_df.empno_tgt) & (scd1_df.sal !=scd1_df.sal_tgt),'Y').otherwise('NA'))
scd1_df.show()

#insert record df

scd_ins=scd2_df.select('empno','empname','deptno','sal').filter(scd2_df.INS_FLAG=='Y')
scd_ins.show()

#update record df

scd_upd=scd2_df.select('empno','empname','deptno','sal').filter(scd2_df.UPD_FLAG=='Y')
scd_upd.show()

#record to be overriden
scd_over=scd2_df.select('empno','empname','deptno','sal').filter((scd2_df.UPD_FLAG!='Y') & (scd2_df.INS_FLAG!='Y'))


df_final=scd_ins.unionAll(scd_upd).unionAll(scd_over)
df_final.show()

df_final.count()

df_final.write.format("jdbc").mode("overwrite").option("url", "jdbc:mysql://localhost:3306/swapnil").option("driver","com.mysql.jdbc.Driver").option("dbtable", "emp_spark_scd1").option("user", "root").option("password", "root").save()






