WITH
-- Chi tiết đơn theo tuần (giữ logic IsOntime)
Details AS (
  SELECT
    week(C.orderdate) AS Time,
    C.ordercode,
    C.fromwardcode AS Ward_id,
    C.fromprovince AS Province,
    C.fromdistrict AS District,
    C.pickwh AS Hub,
    C.fromregionshortname,
    CASE 
      WHEN C.PickWH LIKE '%Ahamove%' THEN 'Ahamove'
      WHEN C.PickWarehouseID IN (1297,1327) THEN 'KHL'
      WHEN C.PickWarehouseID IN (
        SELECT warehouse_id 
        FROM "dw-ghn".datawarehouse.dim_warehouse 
        WHERE department_name = 'Freight Operations Department'
           OR warehouse_name LIKE '%Kho Chuyển Tiếp %'
      ) THEN 'GXT'
      ELSE 'BC'
    END AS TypeKH,
    CASE
      WHEN C.endpicktime IS NOT NULL AND DATE(C.endpicktime) <= DATE(C.orderdate) THEN 1
      WHEN (
        (COALESCE(C.firstfailpicknote, '') = 'Nhân viên gặp sự cố'
         AND C.secondupdatedpickeduptime IS NOT NULL
         AND DATE(C.secondupdatedpickeduptime) <= DATE(C.orderdate))
        OR
        (COALESCE(C.firstfailpicknote, '') != 'Nhân viên gặp sự cố'
         AND COALESCE(C.firstupdatedpickeduptime, C.secondupdatedpickeduptime) IS NOT NULL
         AND DATE(COALESCE(C.firstupdatedpickeduptime, C.secondupdatedpickeduptime)) <= DATE(C.orderdate))
      ) THEN 1
      ELSE 0
    END AS IsOntime,
        CASE
      WHEN 
         DATE(C.endpicktime) is not null and DATE(C.endpicktime) <= DATE(C.orderdate)
      THEN 1
      ELSE 0
    END AS OntimeSuccessPU,
    CASE 
      WHEN 
         DATE(C.endpicktime) is not null then 1   ELSE 0    END AS SuccessPU
    
  FROM "ghn-reporting"."ka"."dtm_ka_v3_createddate" C
  WHERE C.clientid IN (18692)
    AND C.isexpecteddropoff = FALSE
    AND C.channel <> 'WH - Shopee'
    AND DATE(C.orderdate) BETWEEN DATE('2025-01-01') AND CURRENT_DATE - INTERVAL '1' DAY
    AND C.currentstatus <> 'cancel'
),

-- Tổng hợp theo tuần (tránh double count khi join)
DetailsAgg AS (
  SELECT
    Time,
    COUNT(DISTINCT ordercode) AS TotalOrders,
    SUM(IsOntime) AS OntimeOrders,
    sum(OntimeSuccessPU) as OntimeSuccessPU,
    sum(SuccessPU) as SuccessPU,
    COUNT(DISTINCT ordercode) - SUM(IsOntime) AS LateOrders
  FROM Details
  GROUP BY Time
),

-- Đơn đang pending pick theo tuần
Pending_pick AS (
  SELECT 
    week(C.orderdate) AS Time,
    COUNT(*) AS Pending_pick
  FROM "ghn-reporting"."ka"."dtm_ka_v3_createddate" C
  WHERE C.clientid IN (18692)
    AND C.isexpecteddropoff = FALSE
    AND C.channel <> 'WH - Shopee'
    AND DATE(C.orderdate) BETWEEN DATE('2025-01-01') AND CURRENT_DATE - INTERVAL '1' DAY
    AND C.currentstatus IN ('ready_to_pick','picking')
  GROUP BY week(C.orderdate)
),

-- Ngày sự kiện để tính Penalty
event_days AS (
  SELECT CAST(date_column AS DATE) AS Date_SLA
  FROM (
    VALUES 
      -- Double days
      '2024-10-10', '2024-10-11', '2024-10-12', '2024-11-11', '2024-11-12', '2024-11-13', '2024-12-12', '2024-12-13', '2024-12-14',
      '2025-01-15', '2025-01-16', '2025-01-17', '2025-02-02', '2025-02-03', '2025-02-04', '2025-03-03', '2025-03-04', '2025-03-05',
      '2025-04-04', '2025-04-05', '2025-04-06', '2025-05-05', '2025-05-06', '2025-05-07', '2025-06-06', '2025-06-07', '2025-06-08',
      '2025-07-07', '2025-07-08', '2025-07-09', '2025-08-08', '2025-08-09', '2025-08-10', '2025-09-09', '2025-09-10', '2025-09-11',
      '2025-10-10', '2025-10-11', '2025-10-12', '2025-11-11', '2025-11-12', '2025-11-13', '2025-12-12', '2025-12-13', '2025-12-14',
      -- Holidays
      '2025-01-01', '2025-01-25', '2025-01-26', '2025-01-27', '2025-01-28', '2025-01-29', '2025-01-30', '2025-01-31', '2025-02-01',
      '2025-02-02', '2025-04-07', '2025-04-30', '2025-05-01', '2025-09-01', '2025-09-02'
  ) AS t(date_column)
),

-- Tính Penalty theo tuần
Penalty_Raw AS (
  SELECT
    week(C.orderdate) AS Time,
    COUNT(C.ordercode) AS Volume_Created,
    SUM(
      CASE
        WHEN DATE(C.orderdate) = DATE(E.Date_SLA)
             AND DATE(COALESCE(C.firstupdatedpickeduptime, C.endpicktime, CURRENT_DATE)) 
                 <= DATE(C.orderdate + INTERVAL '1' DAY)
        THEN 1
        WHEN DATE(COALESCE(C.firstupdatedpickeduptime, C.endpicktime, CURRENT_DATE)) 
                 <= DATE(C.orderdate)
        THEN 1
        ELSE 0
      END
    ) AS OntimeFirstPU
  FROM dtm_ka_v3_createddate C
  LEFT JOIN event_days AS E
    ON DATE(C.orderdate) = DATE(E.Date_SLA)
  WHERE C.clientid IN (18692)
    AND C.isexpecteddropoff = FALSE
    AND C.channel <> 'WH - Shopee'
    AND DATE(C.orderdate) BETWEEN DATE('2025-10-01') AND CURRENT_DATE - INTERVAL '1' DAY
    AND NOT (C.currentstatus = 'cancel' AND DATE(C.canceltime) <= DATE(C.orderdate))
  GROUP BY week(C.orderdate)
),

-- Chỉ lấy đơn Chủ nhật và tính IsOntimeSunday
SundayDetails AS (
  SELECT
    week(C.orderdate) AS Time,
    C.ordercode,
    CASE
      WHEN C.endpicktime IS NOT NULL AND DATE(C.endpicktime) <= DATE(C.orderdate) THEN 1
      WHEN (
        (COALESCE(C.firstfailpicknote, '') = 'Nhân viên gặp sự cố'
         AND C.secondupdatedpickeduptime IS NOT NULL
         AND DATE(C.secondupdatedpickeduptime) <= DATE(C.orderdate))
        OR
        (COALESCE(C.firstfailpicknote, '') != 'Nhân viên gặp sự cố'
         AND COALESCE(C.firstupdatedpickeduptime, C.secondupdatedpickeduptime) IS NOT NULL
         AND DATE(COALESCE(C.firstupdatedpickeduptime, C.secondupdatedpickeduptime)) <= DATE(C.orderdate))
      ) THEN 1
      ELSE 0
    END AS IsOntimeSunday
  FROM "ghn-reporting"."ka"."dtm_ka_v3_createddate" C
  WHERE C.clientid IN (18692)
    AND C.isexpecteddropoff = FALSE
    AND C.channel <> 'WH - Shopee'
    AND DATE(C.orderdate) BETWEEN DATE('2025-01-01') AND CURRENT_DATE - INTERVAL '1' DAY
    AND C.currentstatus <> 'cancel'
    AND EXTRACT(DAY_OF_WEEK FROM C.orderdate) = 7  -- Kiểm tra: nếu engine coi CN=0 thì đổi thành 0
),

-- Tổng hợp Chủ nhật theo tuần
SundayAgg AS (
  SELECT
    Time,
    COUNT(DISTINCT ordercode) AS TotalSundayOrders,
    SUM(IsOntimeSunday) AS OntimeOrdersSunday,
    COUNT(DISTINCT ordercode) - SUM(IsOntimeSunday) AS LateOrdersSunday
  FROM SundayDetails
  GROUP BY Time
)

SELECT
  D.Time AS Week,
  --COALESCE(P.Pending_pick, 0) AS Pending_pick,
  COALESCE(DA.TotalOrders, 0) AS TotalOrders,
  --COALESCE(DA.OntimeOrders, 0) AS OntimeOrders,
  --COALESCE(DA.LateOrders, 0) AS LateOrders,
     CASE
    WHEN COALESCE(da.TotalOrders, 0) > 0
      THEN CAST(da.OntimeOrders AS DOUBLE) / da.TotalOrders
    ELSE NULL
  END AS Ontime1st,
  
   CASE
    WHEN COALESCE(da.TotalOrders, 0) > 0
      THEN CAST(da.OntimeSuccessPU AS DOUBLE) / da.TotalOrders
    ELSE NULL
  END AS OntimeSuccessPU,
  
   CASE
    WHEN COALESCE(PR.Volume_Created, 0) > 0
      THEN CAST(PR.OntimeFirstPU AS DOUBLE) / PR.Volume_Created
    ELSE NULL
  END AS OntimePenalty, 
  
     CASE
    WHEN COALESCE(da.TotalOrders, 0) > 0
      THEN CAST(da.SuccessPU AS DOUBLE) / da.TotalOrders
    ELSE NULL
  END AS SuccessPU,

  --COALESCE(SA.TotalSundayOrders, 0) AS TotalSundayOrders,
  --COALESCE(SA.OntimeOrdersSunday, 0) AS OntimeOrdersSunday,
  --COALESCE(SA.LateOrdersSunday, 0) AS LateOrdersSunday,
  CASE
    WHEN COALESCE(SA.TotalSundayOrders, 0) > 0
      THEN CAST(SA.OntimeOrdersSunday AS DOUBLE) / SA.TotalSundayOrders
    ELSE NULL
  END AS OntimeRateSunday
FROM (SELECT DISTINCT Time FROM Details) D
LEFT JOIN DetailsAgg DA ON DA.Time = D.Time
LEFT JOIN Pending_pick P ON P.Time = D.Time
LEFT JOIN Penalty_Raw PR ON PR.Time = D.Time
LEFT JOIN SundayAgg SA ON SA.Time = D.Time
WHERE D.Time IN (42,43,44,45,46)
ORDER BY Week;
-------------------------------------------monthly----------------------------------
WITH
-- Chi tiết đơn theo tháng (giữ logic IsOntime và SuccessPU)
Details AS (
  SELECT
    month(C.orderdate) AS Time,
    C.ordercode,
    CASE 
      WHEN C.PickWH LIKE '%Ahamove%' THEN 'Ahamove'
      WHEN C.PickWarehouseID IN (1297,1327) THEN 'KHL'
      WHEN C.PickWarehouseID IN (
        SELECT warehouse_id 
        FROM "dw-ghn".datawarehouse.dim_warehouse 
        WHERE department_name = 'Freight Operations Department'
           OR warehouse_name LIKE '%Kho Chuyển Tiếp %'
      ) THEN 'GXT'
      ELSE 'BC'
    END AS TypeKH,
    CASE
      WHEN C.endpicktime IS NOT NULL AND DATE(C.endpicktime) <= DATE(C.orderdate) THEN 1
      WHEN (
        (COALESCE(C.firstfailpicknote, '') = 'Nhân viên gặp sự cố'
         AND C.secondupdatedpickeduptime IS NOT NULL
         AND DATE(C.secondupdatedpickeduptime) <= DATE(C.orderdate))
        OR
        (COALESCE(C.firstfailpicknote, '') != 'Nhân viên gặp sự cố'
         AND COALESCE(C.firstupdatedpickeduptime, C.secondupdatedpickeduptime) IS NOT NULL
         AND DATE(COALESCE(C.firstupdatedpickeduptime, C.secondupdatedpickeduptime)) <= DATE(C.orderdate))
      ) THEN 1
      ELSE 0
    END AS IsOntime,
    CASE WHEN C.endpicktime IS NOT NULL AND DATE(C.endpicktime) <= DATE(C.orderdate) THEN 1 ELSE 0 END AS OntimeSuccessPU,
    CASE WHEN C.endpicktime IS NOT NULL THEN 1 ELSE 0 END AS SuccessPU
  FROM "ghn-reporting"."ka"."dtm_ka_v3_createddate" C
  WHERE C.clientid IN (18692)
    AND C.isexpecteddropoff = FALSE
    AND C.channel <> 'WH - Shopee'
    AND DATE(C.orderdate) BETWEEN DATE('2025-01-01') AND CURRENT_DATE - INTERVAL '1' DAY
    AND C.currentstatus <> 'cancel'
),

DetailsAgg AS (
  SELECT
    Time,
    COUNT(DISTINCT ordercode) AS TotalOrders,
    sum(IsOntime)*1.0000 AS OntimeOrders,
    sum(OntimeSuccessPU)*1.0000 AS OntimeSuccessPU,
    sum(SuccessPU)*1.0000 AS SuccessPU
  FROM Details
  GROUP BY Time
),

event_days AS (
  SELECT CAST(date_column AS DATE) AS Date_SLA
  FROM (
    VALUES 
      '2025-01-01', '2025-01-25', '2025-01-26', '2025-01-27', '2025-01-28', '2025-01-29', '2025-01-30', '2025-01-31', '2025-02-01',
      '2025-02-02', '2025-04-07', '2025-04-30', '2025-05-01', '2025-09-01', '2025-09-02',
      '2025-10-10', '2025-10-11', '2025-10-12', '2025-11-11', '2025-11-12', '2025-11-13', '2025-12-12', '2025-12-13', '2025-12-14'
  ) AS t(date_column)
),

Penalty_Raw AS (
  SELECT
    month(C.orderdate) AS Time,
    COUNT(DISTINCT ordercode) AS Volume_Created,
    COUNT(DISTINCT CASE
      WHEN DATE(C.orderdate) = DATE(E.Date_SLA)
           AND DATE(COALESCE(C.firstupdatedpickeduptime, C.endpicktime, CURRENT_DATE)) <= DATE(C.orderdate + INTERVAL '1' DAY)
      THEN ordercode
      WHEN DATE(COALESCE(C.firstupdatedpickeduptime, C.endpicktime, CURRENT_DATE)) <= DATE(C.orderdate)
      THEN ordercode
      ELSE NULL
    END) AS OntimeFirstPU
  FROM dtm_ka_v3_createddate C
  LEFT JOIN event_days E ON DATE(C.orderdate) = E.Date_SLA
  WHERE C.clientid IN (18692)
    AND C.isexpecteddropoff = FALSE
    AND C.channel <> 'WH - Shopee'
    AND DATE(C.orderdate) BETWEEN DATE('2025-01-01') AND CURRENT_DATE - INTERVAL '1' DAY
    AND NOT (C.currentstatus = 'cancel' AND DATE(C.canceltime) <= DATE(C.orderdate))
  GROUP BY 1
),

SundayDetails AS (
  SELECT
    month(C.orderdate) AS Time,
    C.ordercode,
    CASE
      WHEN C.endpicktime IS NOT NULL AND DATE(C.endpicktime) <= DATE(C.orderdate) THEN 1
      WHEN (
        (COALESCE(C.firstfailpicknote, '') = 'Nhân viên gặp sự cố'
         AND C.secondupdatedpickeduptime IS NOT NULL
         AND DATE(C.secondupdatedpickeduptime) <= DATE(C.orderdate))
        OR
        (COALESCE(C.firstfailpicknote, '') != 'Nhân viên gặp sự cố'
         AND COALESCE(C.firstupdatedpickeduptime, C.secondupdatedpickeduptime) IS NOT NULL
         AND DATE(COALESCE(C.firstupdatedpickeduptime, C.secondupdatedpickeduptime)) <= DATE(C.orderdate))
      ) THEN 1
      ELSE 0
    END AS IsOntimeSunday
  FROM "ghn-reporting"."ka"."dtm_ka_v3_createddate" C
  WHERE C.clientid IN (18692)
    AND C.isexpecteddropoff = FALSE
    AND C.channel <> 'WH - Shopee'
    AND DATE(C.orderdate) BETWEEN DATE('2025-01-01') AND CURRENT_DATE - INTERVAL '1' DAY
    AND C.currentstatus <> 'cancel'
    AND EXTRACT(DAY_OF_WEEK FROM C.orderdate) = 7
),

SundayAgg AS (
  SELECT
    Time,
    COUNT(DISTINCT ordercode) AS TotalSundayOrders,
    sum(IsOntimeSunday)*1.0000 AS OntimeOrdersSunday
  FROM SundayDetails
  GROUP BY Time
)

SELECT
  DA.Time,
  COALESCE(DA.TotalOrders, 0) AS TotalOrders,

    CASE WHEN DA.TotalOrders > 0
      THEN ROUND(DA.OntimeOrders / DA.TotalOrders, 4)
      ELSE NULL
    END  AS Ontime1st,


    CASE WHEN PR.Volume_Created > 0
      THEN ROUND(PR.OntimeFirstPU / PR.Volume_Created, 4)
      ELSE NULL
    END AS OntimePenalty,

    CASE WHEN DA.TotalOrders > 0
      THEN ROUND(DA.OntimeSuccessPU / DA.TotalOrders, 4)
      ELSE NULL
    END AS OntimeSuccessPU,


    CASE WHEN DA.TotalOrders > 0
      THEN ROUND(DA.SuccessPU  / DA.TotalOrders, 4)
      ELSE NULL
    END AS SuccessPU,


    CASE WHEN SA.TotalSundayOrders > 0
      THEN ROUND(SA.OntimeOrdersSunday / SA.TotalSundayOrders, 4)
      ELSE NULl
    END AS OntimeRateSunday

FROM DetailsAgg DA
LEFT JOIN Penalty_Raw PR ON PR.Time = DA.Time
LEFT JOIN SundayAgg SA ON SA.Time = DA.Time
WHERE DA.Time IN (10,11)
ORDER BY DA.Time;
------------------------------------break theo Region----------------------------------------------------------------

