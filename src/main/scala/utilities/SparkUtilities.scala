package utilities

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import com.devnair.cusin.DataPaths


object SparkUtilities {

  def  getSparkContext(appName:String):SparkContext={
    val conf = new SparkConf().setAppName(appName).setMaster("local")
    // .set("spark.serializer","spark.kryo.registrator")
    val sc = new SparkContext(conf)
    sc
  }

  def getSparkSession(appName:String):SparkSession={
    val spark = SparkSession.builder()
      .appName(appName)
      .master("local")
      //  .config("spark.serializer","spark.kryo.registrator")
      .getOrCreate()

    spark
  }

  def convertCurrencyToDouble(currency:String):Double={
    currency.stripPrefix("$").trim.toDouble
  }

  def getDate(date:String):Timestamp={
    new java.sql.Timestamp(DataPaths.formatter.parseDateTime(date).getMillis)
  }

}
