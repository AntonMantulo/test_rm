{% set partitions_to_replace = [
  'timestamp(current_date)',
  'timestamp(date_sub(current_date, interval 1 day))'
] %}


{{ config(
    materialized='incremental',
    unique_key='transid',
    cluster_by = 'userid',
    incremental_strategy = 'insert_overwrite', 
    partition_by={
      "field": "transactioncompleted",
      "data_type": "timestamp"
    },
    partitions = partitions_to_replace
)}}


SELECT CAST(REGEXP_REPLACE(userid,'[^0-9 ]','') AS INT64) AS userid,
  CASE WHEN transactiontype = 'Deposit'
  THEN 'Deposit'
  ELSE 'Withdraw'
  END AS transactiontype,
  transactioncompleted,
  (creditamount*eurcreditexchangerate) as amounteur,
  creditamount as amount,
  eurcreditexchangerate as eurexchangerate,
  creditcurrency as currency,
  transid,
  lastnote       
FROM sll.transactions
WHERE transactionstatus = 'Success' 
  AND (  transactiontype='Deposit' OR transactiontype='Withdraw' OR transactiontype='User2Agent'  )
  AND creditamount <> 0
  
{% if is_incremental() %}
        AND transactioncompleted in ({{ partitions_to_replace | join(',') }})
    {% endif %}
    
UNION ALL

SELECT p.userid, 
  "Deposit" AS transactiontype,
  transactioncompleted,
  (creditamount*eurcreditexchangerate) as amounteur,
  creditamount as amount,
  eurcreditexchangerate as eurexchangerate,
  creditcurrency as currency,
  t.transid,
  "AMS" as lastnote
FROM sll.transactions as t
JOIN sll.posting as p
ON t.transid = p.transid
WHERE transactiontype = 'Agent2User' 
      AND p.userid != 9702972 and t.userid != '9702810' and p.userid != 10897630 and p.userid != 9683154
      
      {% if is_incremental() %}
        -- recalculate yesterday + today
        AND transactioncompleted >= CURRENT_DATE() -1
        AND postingcompleted >= CURRENT_DATE() -1
            {% endif %}