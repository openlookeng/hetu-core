-- aggregation RIGHT join wrong order
select avg(${database}.${schema}.store_sales_customer_item.ss_item_sk),${database}.${schema}.store_returns.sr_item_sk from ${database}.${schema}.store_sales_customer_item
Right JOIN ${database}.${schema}.store_returns ON
${database}.${schema}.store_sales_customer_item.ss_customer_sk = ${database}.${schema}.store_returns.sr_customer_sk and ${database}.${schema}.store_sales_customer_item.ss_item_sk = ${database}.${schema}.store_returns.sr_item_sk
group by ${database}.${schema}.store_returns.sr_customer_sk, ${database}.${schema}.store_returns.sr_item_sk