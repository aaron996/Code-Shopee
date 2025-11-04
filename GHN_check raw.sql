WITH Detail AS (
  SELECT
  ordercode
  ,currentstatus as "Status"
    ,C.fromregionshortname AS Region
    ,fromprovince as "Province"
    ,fromdistrict as "District"
    ,DATE(C.orderdate) AS "scheduled date" 
    --,COUNT(C.ordercode) OVER (PARTITION BY DATE(C.orderdate), C.fromregionshortname) AS TotalVolume 
    ,firstupdatedpickeduptime
    ,firstcreatedpickeduptime
    ,endpicktime 
    ,firstfailpicknote "1st PU reason"
    ,CASE
      WHEN (firstfailpicknote = 'Nhân viên gặp sự cố' AND DATE(firstupdatedpickeduptime) != DATE(secondcreatedpickeduptime) AND DATE(firstupdatedpickeduptime) > DATE(C.orderdate)) THEN 'Late'
      WHEN (HOUR(C.orderdate) < 18 AND DATE(COALESCE(C.firstupdatedpickeduptime, C.endpicktime, CURRENT_DATE)) <= DATE(C.orderdate))
        OR (HOUR(C.orderdate) >= 18 AND DATE(COALESCE(C.firstupdatedpickeduptime, C.endpicktime, CURRENT_DATE)) <= DATE(C.orderdate + INTERVAL '1' DAY)) THEN 'Ontime'
      ELSE 'Late'
    END AS Check1stPU
    
    
  FROM dtm_ka_v3_createddate C
  WHERE C.clientid IN (18692)
    AND isexpecteddropoff = False
    AND DATE(C.orderdate) = current_date - INTERVAL '1' DAY
    AND NOT C.channel = 'WH - Shopee'
    --AND LOWER(pickwh) LIKE 'bưu cục%'
  )
SELECT
ordercode
,"Status"
  ,"scheduled date"
  ,"Province"
  ,"District"
  ,firstupdatedpickeduptime
  --,firstcreatedpickeduptime
  ,endpicktime
  ,"1st PU reason"
  --,sum(OntimeFirstPU) as "1stOntimePU"
  --, COUNT(*) as TotalVolume
  ,Check1stPU
  --,COALESCE(Region, 'XXX_Total') AS Region
  --,ROUND(SUM(OntimeFirstPU) * 1.0000 / COUNT(*), 4) AS "%OntimeSuccessPU"
FROM Detail
--GROUP BY GROUPING SETS (("Time", Region), ("Time"));
--GROUP BY 1
