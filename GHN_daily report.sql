WITH Detail AS (
  SELECT
    C.fromregionshortname AS Region,
    WEEK(C.orderdate) AS "Time",
    COUNT(C.ordercode) OVER (PARTITION BY WEEK(C.orderdate), C.fromregionshortname) AS TotalVolume,
    CASE
      WHEN (HOUR(C.orderdate) < 18 AND DATE(C.endpicktime) <= DATE(C.orderdate)) 
        OR (HOUR(C.orderdate) >= 18 AND COALESCE(DATE(C.endpicktime), CURRENT_DATE) <= DATE(C.orderdate + INTERVAL '1' DAY)) THEN 1
      ELSE 0 
    END AS OntimeSuccessPU
  FROM dtm_ka_v3_createddate C
  WHERE C.clientid IN (18692)
    AND isexpecteddropoff = FALSE
    AND DATE(C.orderdate) BETWEEN 
        DATE_TRUNC('week', current_date - INTERVAL '7' DAY) + INTERVAL '1' DAY -- Thứ Hai tuần trước
        AND DATE_TRUNC('week', current_date) -- Chủ Nhật tuần trước
    AND NOT C.channel = 'WH - Shopee'
)
SELECT
  "Time",
  COALESCE(Region, 'XXX_Total') AS Region,
  ROUND(SUM(OntimeSuccessPU) * 1.0000 / COUNT(*), 4) AS "%OntimeSuccessPU"
FROM Detail
GROUP BY GROUPING SETS (("Time", Region), ("Time"));
