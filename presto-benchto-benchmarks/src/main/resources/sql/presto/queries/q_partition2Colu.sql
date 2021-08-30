select count(${database}.${schema}.web_returns_partiotion_netloss_returneddatesk.wr_net_loss),${database}.${schema}.web_returns_partiotion_netloss_returneddatesk.wr_net_loss
from ${database}.${schema}.web_returns_partiotion_netloss_returneddatesk
group by ${database}.${schema}.web_returns_partiotion_netloss_returneddatesk.wr_account_credit, ${database}.${schema}.web_returns_partiotion_netloss_returneddatesk.wr_net_loss
