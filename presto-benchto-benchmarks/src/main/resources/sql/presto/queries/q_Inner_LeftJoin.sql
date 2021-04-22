-- SortBased Aggreagation  Inner & Left Join
select avg(${database}.${schema}.store_sales_item_customer.ss_item_sk),${database}.${schema}.store_sales_item_customer.ss_item_sk from  ${database}.${schema}.store_sales_item_customer
INNER JOIN  store_returns ON ${database}.${schema}.store_sales_item_customer.ss_item_sk = store_returns.sr_item_sk
Left JOIN  item ON ${database}.${schema}.store_sales_item_customer.ss_item_sk = item.i_item_sk
group by  ${database}.${schema}.store_sales_item_customer.ss_item_sk, ${database}.${schema}.store_sales_item_customer.ss_customer_sk
order by  ${database}.${schema}.store_sales_item_customer.ss_item_sk, ${database}.${schema}.store_sales_item_customer.ss_customer_sk
