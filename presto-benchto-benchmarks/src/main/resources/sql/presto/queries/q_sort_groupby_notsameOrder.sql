-- sort by is customer, item. group by item (sort aggregation will be selected)
select avg(${database}.${schema}.store_sales_customer_item.ss_item_sk),${database}.${schema}.store_sales_customer_item.ss_item_sk from ${database}.${schema}.store_sales_customer_item
group by ${database}.${schema}.store_sales_customer_item.ss_item_sk