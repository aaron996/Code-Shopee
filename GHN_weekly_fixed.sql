WITH
  Details AS (
    SELECT
      DATE(C.orderdate) AS Time --thay đổi sang Week/Month
      ,    C.fromregionshortname AS Region
      ,pickwh AS Hub
      ,case 
      WHEN c.fromprovince = 'Hồ Chí Minh' then 'Key Account Warehouse Ho Chi Minh'
      else 'Key Account Warehouse Ha Noi'
      end as Ten_hub_kho
      ,fromprovince as Province
      ,fromdistrict as District
      ,fromwardcode AS Ward_id
      ,COUNT(C.ordercode) AS Volume_Created
      ,SUM(
        CASE
          WHEN (firstfailpicknote = 'Nhân viên gặp sự cố' AND DATE(firstupdatedpickeduptime) != DATE(secondcreatedpickeduptime) AND DATE(firstupdatedpickeduptime) >= DATE(C.orderdate)) THEN 0
          WHEN (HOUR(C.orderdate) < 18 
                AND DATE(COALESCE(C.firstupdatedpickeduptime, C.endpicktime)) <= DATE(C.orderdate))
            OR (HOUR(C.orderdate) >= 18 
                AND DATE(COALESCE(C.firstupdatedpickeduptime, C.endpicktime)) <= DATE(C.orderdate + INTERVAL '1' DAY))
          THEN 1
          ELSE 0
        END
      ) AS OntimeFirstPU
    FROM "ghn-reporting"."ka"."dtm_ka_v3_createddate" C
    WHERE C.clientid IN (18692)
      AND C.isexpecteddropoff = FALSE
      AND NOT C.channel = 'WH - Shopee'
      --AND DATE(C.orderdate) BETWEEN DATE_TRUNC('Month',CURRENT_DATE) AND CURRENT_DATE - INTERVAL '1' DAY
      AND DATE(c.orderdate) BETWEEN date_add('day', -60, CURRENT_DATE) AND date_add('day', -1, CURRENT_DATE)
      --AND C.fromprovince  in 
      --('Hà Nội','Hồ Chí Minh')
      --('Hồ Chí Minh')
      --AND C.pickwarehouseid IN (1297,1327,22123000,21452000,21093000,21601000,22075000,22122000)
      AND LOWER(pickwh) LIKE 'bưu cục%'
     --AND LOWER(pickwh) not LIKE 'bưu cục%'
      
    GROUP BY 1, 2,3,4,5,6,7
  )
  SELECT
    Time
    ,Region
    --,Ten_hub_kho
    ,Province
    ,District
    --,w.ward_name as Ward
    ,Hub
    ,sum(Volume_Created) as Vol
    ,sum(OntimeFirstPU) as OntimeFirstPU    
    ,ROUND((AVG(OntimeFirstPU) / AVG(Volume_Created)),4) AS Ontime
    ,ROUND(SUM(OntimeFirstPU) * 1.0000 / COUNT(*), 4) AS "%OntimeSuccessPU"

    
  FROM Details D
  JOIN "dw-ghn"."datawarehouse"."dim_location_ward" W
  ON D.Ward_id = W.ward_id
  where 
week(d.Time) in (43,44)
--BETWEEN date_add('day', -8, CURRENT_DATE) AND date_add('day', -1, CURRENT_DATE)

  --where District in ('Quận 12')
  GROUP BY 1,2,3,4,5
  ORDER BY 1
