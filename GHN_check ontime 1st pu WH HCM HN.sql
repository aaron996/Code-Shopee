WITH
  Details AS (
    SELECT
      DATE(C.orderdate) AS Time --thay đổi sang Week/Month
      ,pickwh AS Hub
      ,case 
      WHEN c.fromprovince = 'Hồ Chí Minh' then 'Key Account Warehouse Ho Chi Minh'
      else 'Key Account Warehouse Ha Noi'
      end as Ten_hub_kho
      ,fromwardcode AS Ward_id
      ,fromdistrict as District
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
      AND DATE(c.orderdate) BETWEEN date_add('day', -7, CURRENT_DATE) AND date_add('day', -1, CURRENT_DATE)
      AND C.fromprovince  in ('Hà Nội','Hồ Chí Minh')
      --AND LOWER(pickwh) LIKE 'bưu cục%'
      AND LOWER(pickwh) not LIKE 'bưu cục%'
      
    GROUP BY 1, 2,3,4,5
  )
  SELECT
    Time
    ,Ten_hub_kho
    --,Hub
    --,District
    --,Ward_id
    ,ROUND((AVG(OntimeFirstPU) / AVG(Volume_Created)),4) AS Ontime
  FROM Details
  GROUP BY 1,2
  ORDER BY 1,2
