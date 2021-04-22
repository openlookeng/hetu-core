--grouping are less when compare to Join.  Inner join item, customer, soldDate.  group :item, customer.  sort:item, customer, soldDate
select avg(${database}.${schema}.store_sales_item_customer_solddate.ss_item_sk),${database}.${schema}.store_sales_item_customer_solddate.ss_item_sk from ${database}.${schema}.store_sales_item_customer_solddate
LEFT JOIN store_returns ON
${database}.${schema}.store_sales_item_customer_solddate.ss_item_sk = store_returns.sr_item_sk
and ${database}.${schema}.store_sales_item_customer_solddate.ss_customer_sk = store_returns.sr_customer_sk
and ${database}.${schema}.store_sales_item_customer_solddate.ss_sold_date_sk = store_returns.sr_returned_date_sk
group by ${database}.${schema}.store_sales_item_customer_solddate.ss_item_sk, ${database}.${schema}.store_sales_item_customer_solddate.ss_customer_sk