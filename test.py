from pyspark.sql import SparkSession

from pyspark.sql.functions import *

from pyspark.sql.window import Window

spark=SparkSession.builder.appName("learning").getOrCreate()

data= [
 ("Alice", "Math", "Semester1", 85),
 ("Alice", "Math", "Semester2", 90),
 ("Alice", "English", "Semester1", 78),
 ("Alice", "English", "Semester2", 82),
 ("Bob", "Math", "Semester1", 65),
 ("Bob", "Math", "Semester2", 70),
 ("Bob", "English", "Semester1", 60),
 ("Bob", "English", "Semester2", 65),
 ("Charlie", "Math", "Semester1", 95),
 ("Charlie", "Math", "Semester2", 98),
 ("Charlie", "English", "Semester1", 88),
 ("Charlie", "English", "Semester2", 90),
 ("David", "Math", "Semester1", 78),
 ("David", "Math", "Semester2", 80),
 ("David", "English", "Semester1", 75),
 ("David", "English", "Semester2", 72),
 ("Eve", "Math", "Semester1", 88),
 ("Eve", "Math", "Semester2", 85),
 ("Eve", "English", "Semester1", 80),
 ("Eve", "English", "Semester2", 83)
]

schema=["Student", "Subject", "Semester", "Grade"]

df=spark.createDataFrame(data,schema)

df.show()

df1=df.groupBy('Student').agg(avg('Grade').alias('AverageGrade'))

df1.show()

df2=df1.filter(col('AverageGrade') >=75)

df2.show()

df3=df.groupBy('Subject').agg(max('Semester').alias('latest_semester')).withColumnRenamed('Subject','Subject_latest')


df4=df.join(df3,(df.Subject==df3.Subject_latest) & (df.Semester==df3.latest_semester),"inner").select("Subject","Student","Grade")

df4.show()

window_spec=Window.partitionBy('Subject').orderBy(col('Grade').desc())

df5=df4.withColumn("rank",dense_rank().over(window_spec)).filter(col('rank')<=3)

df5.show()

df6=df5.orderBy(col('Subject'),col('Grade').desc())

df6.show()


+-------+-------+---------+-----+
|Student|Subject| Semester|Grade|
+-------+-------+---------+-----+
|  Alice|   Math|Semester1|   85|
|  Alice|   Math|Semester2|   90|
|  Alice|English|Semester1|   78|
|  Alice|English|Semester2|   82|
|    Bob|   Math|Semester1|   65|
|    Bob|   Math|Semester2|   70|
|    Bob|English|Semester1|   60|
|    Bob|English|Semester2|   65|
|Charlie|   Math|Semester1|   95|
|Charlie|   Math|Semester2|   98|
|Charlie|English|Semester1|   88|
|Charlie|English|Semester2|   90|
|  David|   Math|Semester1|   78|
|  David|   Math|Semester2|   80|
|  David|English|Semester1|   75|
|  David|English|Semester2|   72|
|    Eve|   Math|Semester1|   88|
|    Eve|   Math|Semester2|   85|
|    Eve|English|Semester1|   80|
|    Eve|English|Semester2|   83|
+-------+-------+---------+-----+

+-------+------------+
|Student|AverageGrade|
+-------+------------+
|Charlie|       92.75|
|    Bob|        65.0|
|  Alice|       83.75|
|    Eve|        84.0|
|  David|       76.25|
+-------+------------+

+-------+------------+
|Student|AverageGrade|
+-------+------------+
|Charlie|       92.75|
|  Alice|       83.75|
|    Eve|        84.0|
|  David|       76.25|
+-------+------------+

+-------+-------+-----+
|Subject|Student|Grade|
+-------+-------+-----+
|English|  Alice|   82|
|English|    Bob|   65|
|   Math|  Alice|   90|
|   Math|    Bob|   70|
|   Math|Charlie|   98|
|English|Charlie|   90|
|English|  David|   72|
|English|    Eve|   83|
|   Math|  David|   80|
|   Math|    Eve|   85|
+-------+-------+-----+

+-------+-------+-----+----+
|Subject|Student|Grade|rank|
+-------+-------+-----+----+
|English|Charlie|   90|   1|
|English|    Eve|   83|   2|
|English|  Alice|   82|   3|
|   Math|Charlie|   98|   1|
|   Math|  Alice|   90|   2|
|   Math|    Eve|   85|   3|
+-------+-------+-----+----+

+-------+-------+-----+----+
|Subject|Student|Grade|rank|
+-------+-------+-----+----+
|English|Charlie|   90|   1|
|English|    Eve|   83|   2|
|English|  Alice|   82|   3|
|   Math|Charlie|   98|   1|
|   Math|  Alice|   90|   2|
|   Math|    Eve|   85|   3|
+-------+-------+-----+----+
