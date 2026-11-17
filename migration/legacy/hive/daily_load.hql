-- Legacy daily transaction load (HiveQL) - kept verbatim as the migration reference.
-- The curated Spark/Scala job (modules/transform) must reproduce exactly this behavior,
-- and the reconciliation job proves it per partition. See docs/migration-guide.md.
--
-- parameters injected by the oozie hive action:
--   inbox   = landing area of the delivered feeds
--   jobDate = yyyy-MM-dd

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.max.split.size=134217728;

-- staging external table over the day's delivered feed
drop table if exists tmp_txn_stage;
create external table tmp_txn_stage (
  txn_id string,
  account_id string,
  direction string,
  amount double,
  currency string,
  channel string,
  counterparty_name string,
  counterparty_bic string,
  counterparty_country string,
  value_date string,
  txn_timestamp string
)
row format delimited fields terminated by ','
location '${hivevar:inbox}/transactions/business_date=${hivevar:jobDate}'
tblproperties ('skip.header.line.count'='1');

-- publish the day's partition: deduplicate on the business key (keep the latest
-- version) and apply the business validity rules
insert overwrite table cur_transactions partition (business_date='${hivevar:jobDate}')
select
  txn_id, account_id, direction, amount, currency, channel,
  counterparty_name, counterparty_bic, counterparty_country, value_date, txn_timestamp
from (
  select t.*,
         row_number() over (partition by txn_id order by txn_timestamp desc) as rn
  from tmp_txn_stage t
) ranked
where rn = 1
  and amount > 0
  and currency != 'XX'
  and account_id != 'ACCT000000';
