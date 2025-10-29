WITH Detail AS (
  SELECT
    C.fromregionshortname AS Region
  ,fromprovince as Province  
  ,fromwardcode AS Ward_id
  ,fromdistrict as District,
    date(C.orderdate) AS "Time",
    CASE
      WHEN (
        HOUR(C.orderdate) < 18 AND DATE(C.endpicktime) <= DATE(C.orderdate)
      ) OR (
        HOUR(C.orderdate) >= 18 AND COALESCE(DATE(C.endpicktime), CURRENT_DATE) <= DATE(C.orderdate + INTERVAL '1' DAY)
      )
      THEN 1
      ELSE 0
    END AS OntimeSuccessPU
    ,CASE
      WHEN (firstfailpicknote = 'Nhân viên gặp sự cố' AND DATE(firstupdatedpickeduptime) != DATE(secondcreatedpickeduptime) AND DATE(firstupdatedpickeduptime) >= DATE(C.orderdate)) THEN 0
      WHEN (HOUR(C.orderdate) < 18 AND DATE(COALESCE(C.firstupdatedpickeduptime, C.endpicktime, CURRENT_DATE)) <= DATE(C.orderdate))
        OR (HOUR(C.orderdate) >= 18 AND DATE(COALESCE(C.firstupdatedpickeduptime, C.endpicktime, CURRENT_DATE)) <= DATE(C.orderdate + INTERVAL '1' DAY)) THEN 1
      ELSE 0
    END AS OntimeFirstPU
  FROM dtm_ka_v3_createddate C
  WHERE
    C.clientid IN (18692)
    AND isexpecteddropoff = FALSE
    AND DATE(C.orderdate) BETWEEN (CURRENT_DATE - INTERVAL '10' DAY) AND (CURRENT_DATE - INTERVAL '1' DAY)
    AND C.channel != 'WH - Shopee'
)

SELECT
  "Time"
  --Province
  --,District
  --,Ward_id
   --,COUNT(*) as vol,
   --SUM(OntimeSuccessPU) as "OntimeSuccessPU",
   --SUM(OntimeFirstPU) as OntimeFirstPU
  -- COALESCE(Region, 'XXX_Total') AS Region,
  ,ROUND(SUM(OntimeSuccessPU) * 1.0000 / COUNT(*), 4) AS "%OntimeSuccessPU"
  ,ROUND(SUM(OntimeFirstPU) * 1.0000 / COUNT(*), 4) AS "%Ontime1stPU"

FROM Detail
GROUP BY 1
ORDER BY 1 DESC;
