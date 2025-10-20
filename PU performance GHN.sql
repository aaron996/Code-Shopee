--1st PU theo tuần
WITH Detail AS (
  SELECT
    C.fromregionshortname AS Region
    ,WEEK(C.orderdate) AS "Time"
    ,COUNT(C.ordercode) OVER (PARTITION BY WEEK(C.orderdate), C.fromregionshortname) AS TotalVolume
    ,CASE
      WHEN (firstfailpicknote = 'Nhân viên gặp sự cố' AND DATE(firstupdatedpickeduptime) != DATE(secondcreatedpickeduptime) AND DATE(firstupdatedpickeduptime) >= DATE(C.orderdate)) THEN 0
      WHEN (HOUR(C.orderdate) < 18 AND DATE(COALESCE(C.firstupdatedpickeduptime, C.endpicktime, CURRENT_DATE)) <= DATE(C.orderdate))
        OR (HOUR(C.orderdate) >= 18 AND DATE(COALESCE(C.firstupdatedpickeduptime, C.endpicktime, CURRENT_DATE)) <= DATE(C.orderdate + INTERVAL '1' DAY)) THEN 1
      ELSE 0
    END AS OntimeFirstPU
  FROM dtm_ka_v3_createddate C
  WHERE C.clientid IN (18692)
    AND isexpecteddropoff = False
    AND 
    DATE(C.orderdate) >= date_trunc('week', date_add('day', -7, current_date))
  AND DATE(C.orderdate) < date_trunc('week', current_date)
    AND NOT C.channel = 'WH - Shopee'
  )
SELECT
  "Time"
  ,COALESCE(Region, 'XXX_Total') AS Region
  ,ROUND(SUM(OntimeFirstPU) * 1.0000 / COUNT(*), 4) AS "%OntimeSuccessPU"
FROM Detail
GROUP BY GROUPING SETS (("Time", Region), ("Time"));

--OPR theo tuần
WITH Detail AS (
  SELECT
    C.fromregionshortname AS Region
    ,WEEK(C.orderdate) AS "Time"
    ,COUNT(C.ordercode) OVER (PARTITION BY WEEK(C.orderdate), C.fromregionshortname) AS TotalVolume
    ,CASE
  WHEN (HOUR(C.orderdate) < 18 AND DATE(C.endpicktime) <= DATE(C.orderdate)) 
    OR (HOUR(C.orderdate) >= 18 AND COALESCE(DATE(C.endpicktime), CURRENT_DATE) <= DATE(date_add('day', 1, C.orderdate))) THEN 1
  ELSE 0 
END OntimeSuccessPU

  FROM dtm_ka_v3_createddate C
  WHERE C.clientid IN (18692)
    AND isexpecteddropoff = False
    AND DATE(C.orderdate) >= date_trunc('week', date_add('day', -7, current_date))
  AND DATE(C.orderdate) < date_trunc('week', current_date)

    AND NOT C.channel = 'WH - Shopee'
  )
SELECT
  "Time"
  ,COALESCE(Region, 'XXX_Total') AS Region
  ,ROUND(SUM(OntimeSuccessPU) * 1.0000 / COUNT(*), 4) AS "%OntimeSuccessPU"
FROM Detail
GROUP BY GROUPING SETS (("Time", Region), ("Time"));
