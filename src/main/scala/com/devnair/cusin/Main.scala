package com.devnair.cusin

import java.sql.Timestamp
import java.time.Month
import utilities.SparkUtilities


case class Customer(cId:Int, fName:String, lName:String, phoneNumber:Long)
case class Product(pId:Int, pName:String, pType:String, pVersion:String, pPrice:Double)
case class Refund(rId:Int, tId:Int, cId:Int, pId:Int, timestamp:Timestamp, refundAmount:Double, refundQuantity:Int)
case class Sales(tId:Int, cId:Int, pId:Int, timestamp:Timestamp, totalAmount:Double, totalQuantity:Int)


object Main {


  def main(args:Array[String]):Unit={

    /* Initialize */
    /* Passing the name of this class as appName */
    val spark = SparkUtilities.getSparkSession(this.getClass.getName)


    val productDF = ReadData.readProductsDF(spark,DataPaths.PRODUCT_PATH)
    productDF.createOrReplaceTempView("products")

    val salesDF = ReadData.readSalesDF(spark,DataPaths.SALES_PATH)
    salesDF.createOrReplaceTempView("sales")

    val refundDF = ReadData.readRefundDF(spark,DataPaths.REFUND_PATH)
    refundDF.createOrReplaceTempView("refunds")

    val customersDF = ReadData.readCustomerDF(spark,DataPaths.CUSTOMER_PATH)
    customersDF.createOrReplaceTempView("customers")

    /* Display the distribution of sales by product name and product type.*/
    KPIFunctions.getDistribution(spark)

    //Calculate the total amount of all transactions that happened in year 2013 and have not been refunded as of today.
    KPIFunctions.calcSalesAmountInYear(spark,salesDF,2013)

    //Display the customer name who made the second most purchases in the month of May 2013. Refunds should be excluded.
    KPIFunctions.calcSecondMostPurchase(spark,salesDF,customersDF,2013,Month.MAY)

    //Find a product that has not been sold at least once (if any).
    //KPIFunctions.findNotPurchasedProducts(spark,productDF,salesDF)
    KPIFunctions.findNotPurchasedProducts(spark)

    //Calculate the total number of users who purchased the same product consecutively at least 2 times on a given day.
    val count = KPIFunctions.countConsecutiveBuyers(spark,salesDF)
    println("Total number of users who purchased the same product consecutively at least 2 times on a given day: "+count)

  }


}