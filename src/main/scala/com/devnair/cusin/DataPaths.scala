package com.devnair.cusin

object DataPaths {


import org.joda.time.format.DateTimeFormat


  val DELIMITER="\\|"
  val COMMADELIMITER=","
  val format= new java.text.SimpleDateFormat("MM-dd-YYYY")
  val formatter = DateTimeFormat.forPattern("MM/dd/YYYY HH:mm:ss");
  val PRODUCT_PATH="src/main/resources/data/Product.txt"
  val SALES_PATH="src/main/resources/data/Sales.txt"
  val REFUND_PATH="src/main/resources/data/Refund.txt"
  val CUSTOMER_PATH="src/main/resources/data/Customer.txt"
  val COUNTRY_DATA="src/main/resources/data/data.txt"
  val BASE_OUTPUT_DIR="src/main/resources/output/customerInsights/"

}