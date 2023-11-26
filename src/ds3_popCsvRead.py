#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import os
import pyspark

def doIt():
    print ("---------RESULT-----------")
    popDf = spark\
                .read.option("charset", "euc-kr")\
                .option("header", "true")\
                .csv(os.path.join("data","경기도 의정부시_인구현황_20230831.csv"))
    popDf.show(5)
    agedDf = spark\
                .read.option("charset", "euc-kr")\
                .option("header", "true")\
                .csv(os.path.join("data","제주특별자치도 서귀포시_고령화비율및노령화지수현황_20230324.csv"))
    agedDf.show(5)

if __name__ == "__main__":
    os.environ["PYSPARK_PYTHON"]="/usr/bin/python3"
    os.environ["PYSPARK_DRIVER_PYTHON"]="/usr/bin/python3"
    myConf=pyspark.SparkConf()
    spark = pyspark.sql.SparkSession.builder\
        .master("local")\
        .appName("myApp")\
        .config(conf=myConf)\
        .getOrCreate()
    doIt()
    spark.stop()
