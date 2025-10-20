-- check ontime PU 1st
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
      AND C.pickwarehouseid IN (1297,1327)
      --AND LOWER(pickwh) LIKE 'bưu cục%'
     -- AND LOWER(pickwh) not LIKE 'bưu cục%'
      
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

-- check ontime PU 1st

WITH
  Raw AS (
    SELECT
      fromwardcode AS Ward_id
      ,clientcontactname AS Shop_Name
      ,pickwh AS Warehouse
      ,ordercode AS "Đơn đại diện"
      ,row_number() OVER (PARTITION BY fromwardcode, clientcontactname ORDER BY DATE(orderdate) DESC) AS Rank_ordercode
    FROM "ghn-reporting"."ka"."dtm_ka_v3_createddate"
    WHERE 1 = 1
      AND clientid = 18692
      AND pickwarehouseid IN (1297,1327)
      AND NOT channel = 'WH - Shopee'
  )
  SELECT
    R.Ward_id
    ,R.Shop_Name
    ,R.Warehouse
    ,R."Đơn đại diện"
    ,W.region_shortname AS Region
    ,W.province_name AS Province
    ,W.district_name as District
    ,W.ward_name AS Ward
  FROM Raw AS R
  JOIN "dw-ghn"."datawarehouse"."dim_location_ward" AS W
    ON R.Ward_id = W.ward_id
  WHERE 1 = 1
    AND Rank_ordercode = 1
