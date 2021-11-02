{% set partitions_to_replace = [
  'current_date',
  'date_sub(current_date, interval 1 day)'
] %}


{{ config(
    materialized='incremental',
    unique_key='postingid',
    cluster_by = 'userid',
    incremental_strategy = 'insert_overwrite', 
    partition_by={
      "field": "postingcompleted",
      "data_type": "timestamp"
    },
    partitions = partitions_to_replace
)}}

WITH master AS (
WITH master1 AS (
WITH d AS (
WITH s AS (
SELECT CAST(TRIM(RIGHT(note, 13)) AS INT64) AS bonuswalletid,
      postingcompleted
FROM sll.posting AS posting
WHERE (note LIKE 'ReturnAmountCausedByCompletion%' 
  OR note LIKE 'ReleaseBonusWallet%'
  OR note LIKE 'ConfiscateBonusCausedByExpiry%'
  OR note LIKE 'ConfiscateBonusCausedByForfeiture%')
  AND payitemname = 'UBS'

{% if is_incremental() %}
        and DATE(postingcompleted) in ({{ partitions_to_replace | join(',') }})
    {% endif %}

)

SELECT *,
  ROW_NUMBER () OVER (PARTITION BY bonuswalletid ORDER BY postingcompleted ASC) AS rn   
FROM s
WHERE bonuswalletid not in (SELECT bonuswalletid FROM dbt_amantulo.bonus_costs WHERE DATE(postingcompleted) < CURRENT_DATE() -1)) 
SELECT * 
FROM d 
WHERE rn = 1

),

bets AS (
SELECT  CAST(TRIM(RIGHT(note, 13)) AS INT64) as bonuswalletid, 
  userid, 
  postingid,
  amount,
  eurexchangerate,
  currency,
  transid
FROM sll.posting
WHERE note like 'DebitBonusWallet%'

{% if is_incremental() %}
      and DATE(postingcompleted) > CURRENT_DATE() -32
  {% endif %}

),

gamefeed AS(
WITH gamefeed AS
(SELECT gameid, 
  gamegroup,
  productname,
  gamename,
  updated,
  ROW_NUMBER () OVER (PARTITION BY gameid ORDER BY updated DESC) AS rn
FROM `stitch-test-296708.sll.gamefeed` 
ORDER BY 1
)
SELECT *
FROM gamefeed
WHERE rn = 1

), 

gamingtrans AS(
WITH gamingtrans AS
(SELECT *,
  ROW_NUMBER () OVER (PARTITION BY transid) AS rn
FROM `stitch-test-296708.sll.gamingtrans` 
)
SELECT *
FROM gamingtrans
WHERE rn = 1

), 

gamingtrans2 AS (
WITH gamingtrans AS
(SELECT transid,
  matchingpostingid,
  ROW_NUMBER () OVER (PARTITION BY transid) AS rn
FROM `stitch-test-296708.sll.gamingtrans` 
)
SELECT matchingpostingid, MAX(postingcompleted) as postingcompleted
FROM gamingtrans
JOIN sll.posting
ON gamingtrans.transid = posting.transid
WHERE rn = 1 and matchingpostingid is not null

{% if is_incremental() %}
      and DATE(postingcompleted) > CURRENT_DATE() -32
  {% endif %}

GROUP BY 1)


SELECT master1.bonuswalletid,
  'BonusMoney' as wallettype,
  bets.userid,
  gamegroup,
  productname,
  gamename,
  bets.postingid,
  master1.postingcompleted,
  bets.amount * bets.eurexchangerate as amounteur,
  bets.amount,
  bets.currency,
  bets.eurexchangerate,
  sessionid
FROM master1
LEFT JOIN bets 
ON master1.bonuswalletid = bets.bonuswalletid
LEFT JOIN gamingtrans
ON bets.transid = gamingtrans.transid
LEFT JOIN gamefeed
ON gamingtrans.gameid = gamefeed.gameid
WHERE postingid is not null

UNION ALL 

SELECT 
  CASE 
    WHEN note like 'DebitRealMoney%' and note != 'DebitRealMoney. BonusWalletID = 0'
    THEN CAST(TRIM(RIGHT(posting.note, 13)) AS INT64)
    ELSE null
   END as bonuswalletid,
  'RealCash' as wallettype,
  posting.userid,
  gamegroup,
  productname,
  gamename,
  posting.postingid,
  CASE 
    WHEN gamename = 'Sports Betting'
    THEN gamingtrans2.postingcompleted
    ELSE posting.postingcompleted
  END as postingcompleted,
  posting.amount * posting.eurexchangerate AS amounteur,
  posting.amount,
  posting.currency,
  posting.eurexchangerate,
  gamingtrans.sessionid
FROM `stitch-test-296708.sll.posting` as posting
LEFT JOIN gamingtrans
ON posting.transid = gamingtrans.transid
LEFT JOIN gamefeed
ON gamingtrans.gameid = gamefeed.gameid
LEFT JOIN gamingtrans2
ON posting.postingid = gamingtrans2.matchingpostingid
WHERE posting.paymenttype = 'Debit' and (posting.note is null or posting.note = '' or posting.note like 'DebitRealMoney%' or posting.note like 'Closed%')

{% if is_incremental() %}
        and DATE(posting.postingcompleted) > CURRENT_DATE() - 32
    {% endif %} 
)

SELECT *
FROM master 
WHERE postingcompleted is not null

{% if is_incremental() %}
        and DATE(postingcompleted) in ({{ partitions_to_replace | join(',') }})
    {% endif %}