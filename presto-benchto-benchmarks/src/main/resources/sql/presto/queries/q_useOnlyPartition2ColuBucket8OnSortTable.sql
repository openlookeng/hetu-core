select count(${database}.${schema}.web_returns_partiotion_netloss_returneddatesk_bucket8_sort.wr_net_loss),${database}.${schema}.web_returns_partiotion_netloss_returneddatesk_bucket8_sort.wr_net_loss
from ${database}.${schema}.web_returns_partiotion_netloss_returneddatesk_bucket8_sort
group by ${database}.${schema}.web_returns_partiotion_netloss_returneddatesk_bucket8_sort.wr_account_credit, ${database}.${schema}.web_returns_partiotion_netloss_returneddatesk_bucket8_sort.wr_net_loss
