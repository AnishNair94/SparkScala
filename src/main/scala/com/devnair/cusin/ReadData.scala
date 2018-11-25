package com.devnair.cusin

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import utilities.SparkUtilities

object ReadData {


  def readProductsDF(spark:SparkSession,path:String):DataFrame={
    import spark.implicits._
    spark.read.textFile(path)
      .map( line => line.split(DataPaths.DELIMITER))
      .map(fields => new Product(fields(0).toInt, fields(1), fields(2), fields(3),SparkUtilities.convertCurrencyToDouble(fields(4))))
      .toDF()
  }

  def readSalesDF(spark:SparkSession,path:String):DataFrame= {
    import spark.implicits._
    spark.read.textFile(DataPaths.SALES_PATH)
      .map(line => line.split(DataPaths.DELIMITER))
      .map(fields => new Sales(fields(0).toInt,fields(1).toInt,fields(2).toInt, SparkUtilities.getDate(fields(3)), SparkUtilities.convertCurrencyToDouble(fields(4)),fields(5).toInt))
      .toDF()
  }

  def readRefundDF(spark:SparkSession,path:String)={
    import spark.implicits._
    spark.read.textFile(path)
      .map(line => line.split(DataPaths.DELIMITER))
      .map(fields => new Refund(fields(0).toInt,fields(1).toInt,fields(2).toInt,fields(3).toInt, SparkUtilities.getDate(fields(4)), SparkUtilities.convertCurrencyToDouble(fields(5)),fields(6).toInt))
      .toDF()
  }
  def readCustomerDF(spark:SparkSession,path:String)= {
    import spark.implicits._
    spark.read.textFile(path)
      .map( line => line.split(DataPaths.DELIMITER))
      .map(fields => new Customer(fields(0).toInt, fields(1), fields(2), fields(3).toLong))
      .toDF()
  }

  def readCountryDF(spark:SparkSession,path:String):DataFrame={
    import  spark.implicits._

    spark.read.textFile(path)
      .map(line => line.split(DataPaths.COMMADELIMITER))
      .filter(line => line(2) == "Approved")
      .map(line=> new CountryData(line(0).toInt, line(1),line(2),line(3),line(4).toInt))
      .toDF()
  }


  def writeDF(df:DataFrame,path:String):Unit={
    df.coalesce(1)
      .write
      .format("csv")
      .option("header","true")
      .mode(SaveMode.Overwrite)
      .save(path)

  }


}