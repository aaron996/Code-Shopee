WITH Detail AS (
  SELECT
    C.fromregionshortname AS Region,
    MONTH(C.orderdate) AS "Time",
    CASE
      WHEN (firstfailpicknote = 'Nhân viên gặp sự cố' 
            AND DATE(firstupdatedpickeduptime) != DATE(secondcreatedpickeduptime) 
            AND DATE(firstupdatedpickeduptime) >= DATE(C.orderdate)) THEN 0
      WHEN (HOUR(C.orderdate) < 18 
            AND DATE(COALESCE(C.firstupdatedpickeduptime, C.endpicktime, CURRENT_DATE)) <= DATE(C.orderdate))
        OR (HOUR(C.orderdate) >= 18 
            AND DATE(COALESCE(C.firstupdatedpickeduptime, C.endpicktime, CURRENT_DATE)) <= DATE(C.orderdate + INTERVAL '1' DAY)) THEN 1
      ELSE 0
    END AS OntimeFirstPU
  FROM dtm_ka_v3_createddate C
  WHERE C.clientid IN (18692)
    AND isexpecteddropoff = False
    --AND DATE(C.orderdate) BETWEEN date_trunc('month', current_date) 
                              --AND current_date - INTERVAL '1' DAY
    AND DATE(C.orderdate) BETWEEN date('2025-10-01')
    AND date('2025-10-27')
    AND NOT C.channel = 'WH - Shopee'
)
SELECT
  COUNT(*) AS TotalOrders,                  -- Mẫu số
  SUM(OntimeFirstPU) AS Ontime1stPU,        -- Phần tử
ROUND(CAST(SUM(OntimeFirstPU) AS DOUBLE) / NULLIF(COUNT(*), 0), 4) AS "%OntimeSuccessPU"
FROM Detail;
