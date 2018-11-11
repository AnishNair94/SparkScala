package com.devnair.cusin


import java.sql.Timestamp
import java.time.Month

import org.apache.spark.sql.{DataFrame, SparkSession}


object KPIFunctions {

//Display the distribution of sales by product name and product type.
  def getDistribution(spark:SparkSession):Unit={
    import spark.sql
    import org.apache.spark.sql.functions._

    val joinPS = sql("SELECT * FROM products JOIN sales ON products.pId=sales.pId ")
    val groupedPS = joinPS.groupBy("pName","pType")

    val sumGroupedPP = groupedPS.agg(sum("totalAmount").as("TotalPurchase"),sum("totalQuantity").as("TotalQuantity"))
    ReadData.writeDF(sumGroupedPP,DataPaths.BASE_OUTPUT_DIR+"SumAgg")

    val meanGroupedPP = groupedPS.agg(mean("totalAmount").as("AvgPurchase"),mean("totalQuantity").as("AvgQuantity"))
    ReadData.writeDF(meanGroupedPP,DataPaths.BASE_OUTPUT_DIR+"MeanAgg")

    val minGroupedPP = groupedPS.agg(min("totalAmount").as("MinPurchase"),min("totalQuantity").as("MinQuantity"))
    ReadData.writeDF(minGroupedPP,DataPaths.BASE_OUTPUT_DIR+"MinAgg")

    val maxGroupedPP = groupedPS.agg(max("totalAmount").as("MaxPurchase"),max("totalQuantity").as("MaxQuantity"))
    ReadData.writeDF(maxGroupedPP,DataPaths.BASE_OUTPUT_DIR+"MaxAgg")
  }

  //Calculate the total amount of all transactions that happened in year 2013 and have not been refunded as of today.
  def calcSalesAmountInYear(spark:SparkSession,salesDF:DataFrame,yearFilter:Int):Unit={
    import spark.sql
    import org.apache.spark.sql.functions._

    val salesInYear = salesDF.filter(sale => sale.getAs[Timestamp]("timestamp").toLocalDateTime.getYear == yearFilter)
    salesInYear.createOrReplaceTempView("salesInYear")

    val salesInYearUnRefunded = sql("select * from salesInYear where tId NOT IN (select tId from refunds)")
    val salesInYearUnRefundedSum = salesInYearUnRefunded.agg(sum("totalAmount").as("Total Unrefunded Amount"))

    ReadData.writeDF(salesInYearUnRefundedSum,DataPaths.BASE_OUTPUT_DIR+"UnRefundedSum")
  }

  //Display the customer name who made the second most purchases in the month of May 2013. Refunds should be excluded.
  def calcSecondMostPurchase(spark:SparkSession,salesDF:DataFrame,customersDF:DataFrame,yearFilter:Int,monthFilter:Month):Unit={
    import spark.sql
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val salesYearMonthFilter = salesDF.filter(sale =>  sale.getAs[Timestamp]("timestamp").toLocalDateTime.getMonth == monthFilter && sale.getAs[Timestamp]("timestamp").toLocalDateTime.getYear == yearFilter)
    salesYearMonthFilter.createOrReplaceTempView("salesYearMonthFilter")

    val purchases = salesYearMonthFilter.groupBy($"cId").agg(sum($"totalAmount").as("totalPurchases")).orderBy($"totalPurchases".desc)
    val cid = purchases.takeAsList(2).get(1).get(0)

    val result = customersDF.filter(customer => customer.getAs[Int]("cId") == cid)
      .select($"fName",$"lName")

    ReadData.writeDF(result,DataPaths.BASE_OUTPUT_DIR+"SecondMostPurchase")
  }

  //Find a product that has not been sold at least once (if any).
  def findNotPurchasedProducts(spark:SparkSession,productDF:DataFrame,salesDF:DataFrame):Unit={
    import spark.sql
    val productNotPurchased = sql("select * from products where pId NOT IN (select pId from sales)")
    ReadData.writeDF(productNotPurchased,DataPaths.BASE_OUTPUT_DIR+"ProductNotPurchased")
  }

  //Calculate the total number of users who purchased the same product consecutively at least 2 times on a given day.
  def countConsecutiveBuyers(spark:SparkSession,salesDF:DataFrame):Long={
    import spark.implicits._
    val salesPair=salesDF.map(sale => ((sale.getAs[Int]("cId"),sale.getAs[Int]("pId")),sale.getAs[Timestamp]("timestamp")))
      .rdd
      .groupByKey()
      .filter(x => x._2.size>1)
      .mapValues(dates => dates.map(date => DataPaths.format.format(date.getTime)))

    val sameDayCount=salesPair.map {
      x =>
        val cid = x._1._1
        val times = x._2.asInstanceOf[List[String]]
        val lsize = times.size
        val setSize = times.distinct.size
        (cid, lsize-setSize)
    }
      .reduceByKey((x,y)=>x+y)
      .filter(x => x._2>0)
      .count()

    sameDayCount
  }

}