Given a dataset of user content, you need to convert the first letter of each word in the content_text column to 
uppercase while keeping the rest of the letters lowercase. You should display three columns:

The content_id.
The original content_text.
The modified content_text with the first letter of each word in uppercase.
----------------------------------------------------------------------------
Solution:
-----------------------------------------------------------------------------
from pyspark.sql import SparkSession

from pyspark.sql.functions import *

spark=SparkSession.builder.appName("learning").getOrCreate()

data = [
    (1, 2, 'comment', 'hello world! this is a TEST.'),
    (2, 8, 'comment', 'what a great day'),
    (3, 4, 'comment', 'WELCOME to the event.'),
    (4, 2, 'comment', 'e-commerce is booming.'),
    (5, 6, 'comment', 'Python is fun!!'),
    (6, 6, 'review', '123 numbers in text.'),
    (7, 10, 'review', 'special chars: @#$$%^&*()'),
    (8, 4, 'comment', 'multiple CAPITALS here.'),
    (9, 6, 'review', 'sentence. and ANOTHER sentence!'),
    (10, 2, 'post', 'goodBYE!')
]

columns = ["content_id", "customer_id", "content_type", "content_text"]

df=spark.createDataFrame(data,columns)

df.show()

df1=df.withColumn("modified_text",initcap("content_text"))

df1.show()

df1.select("content_id","content_text","modified_text").show(truncate=False)

-------------------------------------------------------------------------
Output
--------------------------------------------------------------------------

+----------+-----------+------------+--------------------+
|content_id|customer_id|content_type|        content_text|
+----------+-----------+------------+--------------------+
|         1|          2|     comment|hello world! this...|
|         2|          8|     comment|    what a great day|
|         3|          4|     comment|WELCOME to the ev...|
|         4|          2|     comment|e-commerce is boo...|
|         5|          6|     comment|     Python is fun!!|
|         6|          6|      review|123 numbers in text.|
|         7|         10|      review|special chars: @#...|
|         8|          4|     comment|multiple CAPITALS...|
|         9|          6|      review|sentence. and ANO...|
|        10|          2|        post|            goodBYE!|
+----------+-----------+------------+--------------------+

+----------+-----------+------------+--------------------+--------------------+
|content_id|customer_id|content_type|        content_text|       modified_text|
+----------+-----------+------------+--------------------+--------------------+
|         1|          2|     comment|hello world! this...|Hello World! This...|
|         2|          8|     comment|    what a great day|    What A Great Day|
|         3|          4|     comment|WELCOME to the ev...|Welcome To The Ev...|
|         4|          2|     comment|e-commerce is boo...|E-commerce Is Boo...|
|         5|          6|     comment|     Python is fun!!|     Python Is Fun!!|
|         6|          6|      review|123 numbers in text.|123 Numbers In Text.|
|         7|         10|      review|special chars: @#...|Special Chars: @#...|
|         8|          4|     comment|multiple CAPITALS...|Multiple Capitals...|
|         9|          6|      review|sentence. and ANO...|Sentence. And Ano...|
|        10|          2|        post|            goodBYE!|            Goodbye!|
+----------+-----------+------------+--------------------+--------------------+

+----------+-------------------------------+-------------------------------+
|content_id|content_text                   |modified_text                  |
+----------+-------------------------------+-------------------------------+
|1         |hello world! this is a TEST.   |Hello World! This Is A Test.   |
|2         |what a great day               |What A Great Day               |
|3         |WELCOME to the event.          |Welcome To The Event.          |
|4         |e-commerce is booming.         |E-commerce Is Booming.         |
|5         |Python is fun!!                |Python Is Fun!!                |
|6         |123 numbers in text.           |123 Numbers In Text.           |
|7         |special chars: @#$$%^&*()      |Special Chars: @#$$%^&*()      |
|8         |multiple CAPITALS here.        |Multiple Capitals Here.        |
|9         |sentence. and ANOTHER sentence!|Sentence. And Another Sentence!|
|10        |goodBYE!                       |Goodbye!                       |
+----------+-------------------------------+-------------------------------+
