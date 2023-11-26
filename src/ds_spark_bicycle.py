import pyspark
import pyspark.sql.functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def classifyQuarter(s):
    q=""
    s=int(s)
    if 1<=s and s< 4:
        q="Q1"
    elif 4<=s and s<7:
        q="Q2"
    elif 7<=s and s<10:
        q="Q3"
    elif 10<=s and s<=12:
        q="Q4"
    else:
        q="no"
    return q

def doIt():
    # create dataframe
	_bicycle = spark.read.format('com.databricks.spark.csv')\
		.options(header='true', inferschema='true').load('data/seoulBicycleDailyCount_2018_201903.csv')
    # columns Date, Count
	bicycle=_bicycle\
		.withColumnRenamed("date", "Date")\
		.withColumnRenamed(" count", "Count")
	#bicycle=bicycle.withColumn("year", bicycle.Date.substr(1, 4))
	#bicycle=bicycle.withColumn("month",bicycle.Date.substr(6, 2))
	bicycle = bicycle\
		.withColumn('year', F.year('date'))\
		.withColumn('month', F.month('date'))

    # column quarter
	quarter_udf = udf(classifyQuarter, StringType())
	bicycle=bicycle.withColumn("quarter", quarter_udf(bicycle.month))
    
    # groupBy
	bicycle.groupBy('quarter').count().show()
	bicycle.groupBy('year').agg({"count":"sum"}).show()
	bicycle.groupBy('quarter').agg({"count":"sum"}).show()
	bicycle.groupBy('quarter').agg({"count":"avg"}).show()
	bicycle.groupBy('year').pivot('month').agg({"count":"sum"}).show()
	bicycle.groupBy('year').pivot('quarter').agg({"count":"sum"}).show()
	sumMonthly=bicycle.groupBy('year').pivot('month').agg({"count":"sum"})
    
    # graph
	pdf=sumMonthly.toPandas()
	my=pdf.drop('year', 1).transpose()
	my.columns=[2018, 2019]
	my.plot(kind='line')

if __name__ == "__main__":
    myConf=pyspark.SparkConf()
    spark = pyspark.sql.SparkSession.builder\
        .master("local")\
        .appName("myApp")\
        .config(conf=myConf)\
        .getOrCreate()
    doIt()
    spark.stop() 
