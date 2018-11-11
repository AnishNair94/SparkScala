# SparkScala
Spark Scala Application: Customer Analytics

Data Set Schema:

Product (product_id, product_name, product_type, product_version, product_price)
Customer (customer_id, customer_first_name, customer_last_name, phone_number )
Sales (transaction_id, customer_id, product_id, timestamp, total_amount, total_quantity )
Refund (refund_id, original_transaction_id, customer_id, product_id, timestamp, refund_amount, refund_quantity)

KPI's:

-> Display the distribution of sales by product name and product type.

-> Calculate the total amount of all transactions that happened in year 2013 and have not been refunded as of today.

-> Display the customer name who made the second most purchases in the month of May 2013. Refunds should be excluded.

-> Find a product that has not been sold at least once (if any).

-> Calculate the total number of users who purchased the same product consecutively at least 2 times on a given day.
