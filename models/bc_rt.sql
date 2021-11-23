{% set partitions_to_replace = [
  'current_date',
  'date_sub(current_date, interval 1 day)'
] %}


{{ config(
    materialized='incremental',
    unique_key='bonuswalletid',
    cluster_by = 'userid',
    incremental_strategy = 'insert_overwrite', 
    partition_by={
      "field": "postingcompleted",
      "data_type": "timestamp"
    },
    partitions = partitions_to_replace
)}}



WITH master AS (
WITH master AS (
WITH d as (

SELECT 
  CAST(TRIM(RIGHT(posting.note, 13)) AS INT64) as bonuswalletid, 
  CASE
    WHEN note like 'ReleaseBonusWallet%'
    THEN 'released'
    WHEN note like 'ReturnAmountCausedByCompletion%'
    THEN 'used-up'
    WHEN note like 'ConfiscateBonusCausedByExpiry%'
    THEN 'expired'
    WHEN note like 'ConfiscateBonusCausedByForfeiture%'
    THEN 'forfeited'
    ELSE ''
  END as bonus_status, 
  *, 
  ROW_NUMBER () OVER (PARTITION BY CAST(TRIM(RIGHT(posting.note, 13)) AS INT64)) as rn
FROM sll.posting
WHERE ((note like 'ReleaseBonusWallet%' and payitemname='UBS')
  OR (note like 'ReturnAmountCausedByCompletion%' and payitemname='UBS')
  OR (note like 'ConfiscateBonusCausedByExpiry%' and payitemname='UBS')
  OR (note like 'ConfiscateBonusCausedByForfeiture%' and payitemname='UBS'))
  
  {% if is_incremental() %}
        and DATE(postingcompleted) in ({{ partitions_to_replace | join(',') }})
    {% endif %}

  
  
  )

SELECT bonuswalletid, 
  bonus_status,
  postingcompleted,
  amount,
  eurexchangerate,
  userid
FROM D 
WHERE rn = 1),


bonus_granted AS (
  WITH type as (
    SELECT DISTINCT 
      bonuswalletid,
      CASE 
        WHEN gamename = 'Sports Betting'
        THEN 'sport'
        ELSE 'casino'
      END as type
    FROM {{ref ('bets_rt') }}

    {% if is_incremental() %}
        WHERE DATE(postingcompleted) >= CURRENT_DATE() -32
    {% endif %}

  )

  SELECT 
    userid,
    CAST(TRIM(RIGHT(note, 13)) AS INT64) AS bonuswalletid,
    amount * eurexchangerate AS bonus_granted,
    postingcompleted as granted,
    amount,
    eurexchangerate,
    currency,
    type
  FROM sll.posting
  LEFT JOIN type
  ON CAST(TRIM(RIGHT(note, 13)) AS INT64) = type.bonuswalletid
  WHERE note like 'GrantBonus%'   
    AND note is not null
    AND postingtype = 'Bonus'
    AND payitemname = 'UBS'

    {% if is_incremental() %}
        AND DATE(postingcompleted) >= CURRENT_DATE() -32
      {% endif %}
    
    
),

ubw AS (
SELECT CAST(TRIM(RIGHT(note, 13)) AS INT64) as bonuswalletid, 
  SUM(amount * eurexchangerate) as unpaid_bonus_winnings
FROM `stitch-test-296708.sll.posting` 
WHERE  (note like 'ConfiscateWinningsCausedByForfeiture%' 
  or note like 'ConfiscateWinningsCausedByExpiry%'
  or note like 'ConfiscateOverCapBonusWin%')
    and payitemname = 'UBS'

    {% if is_incremental() %}
        and DATE(postingcompleted) >= CURRENT_DATE() -32
      {% endif %}
GROUP BY 1
    )

SELECT 
  bonus_granted.userid, 
  bonus_granted.bonuswalletid, 
  CASE
    WHEN bonus_status = 'released' AND type.type IS NULL
    THEN 'WR0'
    ELSE bonus_status
  END as bonus_status,
  bonus_granted.bonus_granted,
  bonus_granted.granted,
  CASE
    WHEN bonus_status = 'released' or bonus_status = 'used-up'
    THEN bonus_granted.amount
    ELSE bonus_granted.amount - IFNULL(master.amount, 0)
  END as amount,
  master.eurexchangerate,
  CASE
    WHEN bonus_status = 'released' or bonus_status = 'used-up'
    THEN bonus_granted.bonus_granted
    ELSE bonus_granted.bonus_granted - IFNULL((master.amount * master.eurexchangerate), 0)
  END as amounteur,
  master.postingcompleted,
  CASE
    WHEN type.type IS NULL
    THEN 'na'
    ELSE type.type
  END AS type,
  ubw.unpaid_bonus_winnings
FROM bonus_granted
LEFT JOIN master
ON bonus_granted.bonuswalletid = master.bonuswalletid
LEFT JOIN ubw
ON bonus_granted.bonuswalletid = ubw.bonuswalletid

)

SELECT *
FROM master 

    {% if is_incremental() %}
        WHERE (DATE(postingcompleted) in ({{ partitions_to_replace | join(',') }}) or date(granted)  >= CURRENT_DATE() -1)
          and bonuswalletid not in (SELECT bonuswalletid FROM `stitch-test-296708.dbt_amantulo.bonus_costs` WHERE postingcompleted is not null and DATE(postingcompleted) < CURRENT_DATE() -2)
      {% endif %}