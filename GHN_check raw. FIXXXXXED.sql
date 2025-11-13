WITH
  Details AS (
    SELECT
      date(C.orderdate) AS Time,
      C.ordercode,
      c.fromwardcode AS Ward_id,
      c.fromprovince AS Province,
      c.fromdistrict as District,
      c.pickwh as Hub,
      case when
      LOWER(c.pickwh) LIKE 'bưu cục%' then 'Vùng' else 'KHL/GXT'end as TypeKH,
CASE
  -- Ưu tiên 1: endpicktime <= orderdate (Thành công ngay trong ngày D)
  WHEN endpicktime IS NOT NULL AND DATE(endpicktime) <= DATE(orderdate) THEN 'Ontime'

  -- Ưu tiên 2: Giờ đặt < 18h (Deadline: cuối ngày D)
  WHEN HOUR(orderdate) < 18 AND (
    (
      COALESCE(firstfailpicknote, '') = 'Nhân viên gặp sự cố' AND
      secondupdatedpickeduptime IS NOT NULL AND DATE(secondupdatedpickeduptime) <= DATE(orderdate)
    )
    OR (
      COALESCE(firstfailpicknote, '') != 'Nhân viên gặp sự cố' AND
      COALESCE(firstupdatedpickeduptime, secondupdatedpickeduptime) IS NOT NULL AND
      DATE(COALESCE(firstupdatedpickeduptime, secondupdatedpickeduptime)) <= DATE(orderdate)
    )
  ) THEN 'Ontime'

  -- Ưu tiên 3: Giờ đặt >= 18h (Deadline: cuối ngày D + 1)
  WHEN HOUR(orderdate) >= 18 AND (
    (
      COALESCE(firstfailpicknote, '') = 'Nhân viên gặp sự cố' AND
      secondupdatedpickeduptime IS NOT NULL AND DATE(secondupdatedpickeduptime) <= DATE(orderdate + INTERVAL '1' DAY)
    )
    OR (
      COALESCE(firstfailpicknote, '') != 'Nhân viên gặp sự cố' AND
      COALESCE(firstupdatedpickeduptime, secondupdatedpickeduptime) IS NOT NULL AND
      DATE(COALESCE(firstupdatedpickeduptime, secondupdatedpickeduptime)) <= DATE(orderdate + INTERVAL '1' DAY)
    )
  ) THEN 'Ontime'

  -- Các trường hợp còn lại
  ELSE 'Late'

      END AS IsOntime,
      firstupdatedpickeduptime,
      secondupdatedpickeduptime,
      firstfailpicknote,
      lastfailpicknote,
      endpicktime,
      firstcreatedpickeduptime
    FROM "ghn-reporting"."ka"."dtm_ka_v3_createddate" C
    WHERE C.clientid IN (18692)
      AND C.isexpecteddropoff = FALSE
      AND NOT C.channel = 'WH - Shopee'
      AND DATE(C.orderdate) BETWEEN CURRENT_DATE - INTERVAL '14' DAY AND CURRENT_DATE - INTERVAL '1' DAY
      AND c.currentstatus != 'cancel'
  )
SELECT
  Time,
  ordercode,
  TypeKH,
  Province,
  District,
  Hub,
  firstupdatedpickeduptime,
  firstfailpicknote,
  endpicktime,
  firstcreatedpickeduptime,
  IsOntime
  --COUNT(ordercode) AS TotalOrders,
  --SUM(IsOntime) AS OntimeOrders,
  --COUNT(ordercode) - SUM(IsOntime) AS LateOrders
FROM Details
WHERE 
Province = 'Hồ Chí Minh' and
District = 'Thành Phố Thủ Đức' and
   --Time BETWEEN CURRENT_DATE - INTERVAL '17' DAY AND CURRENT_DATE - INTERVAL '1' DAY
    Time BETWEEN CURRENT_DATE - INTERVAL '8' DAY and CURRENT_DATE
--GROUP BY Time;


--------------------------------------------------------check số absolute----------------------------------------------
WITH
  Details AS (
    SELECT
      date(C.orderdate) AS Time,
      C.ordercode,
      c.fromwardcode AS Ward_id,
      c.fromprovince AS Province,
      c.fromdistrict as District,
      c.pickwh as Hub,
      c.fromregionshortname,
      case when 
      LOWER(pickwh) LIKE 'bưu cục%' then 'Vùng' else 'KHL/GXT'
      end as TypeKH,
CASE
  -- Ưu tiên 1: endpicktime <= orderdate (Thành công ngay trong ngày D)
  WHEN endpicktime IS NOT NULL AND DATE(endpicktime) <= DATE(orderdate) THEN 1

  -- Ưu tiên 2: Giờ đặt < 18h (Deadline: cuối ngày D)
  WHEN HOUR(orderdate) < 18 AND (
    (
      COALESCE(firstfailpicknote, '') = 'Nhân viên gặp sự cố' AND
      secondupdatedpickeduptime IS NOT NULL AND DATE(secondupdatedpickeduptime) <= DATE(orderdate)
    )
    OR (
      COALESCE(firstfailpicknote, '') != 'Nhân viên gặp sự cố' AND
      COALESCE(firstupdatedpickeduptime, secondupdatedpickeduptime) IS NOT NULL AND
      DATE(COALESCE(firstupdatedpickeduptime, secondupdatedpickeduptime)) <= DATE(orderdate)
    )
  ) THEN 1

  -- Ưu tiên 3: Giờ đặt >= 18h (Deadline: cuối ngày D + 1)
  WHEN HOUR(orderdate) >= 18 AND (
    (
      COALESCE(firstfailpicknote, '') = 'Nhân viên gặp sự cố' AND
      secondupdatedpickeduptime IS NOT NULL AND DATE(secondupdatedpickeduptime) <= DATE(orderdate + INTERVAL '1' DAY)
    )
    OR (
      COALESCE(firstfailpicknote, '') != 'Nhân viên gặp sự cố' AND
      COALESCE(firstupdatedpickeduptime, secondupdatedpickeduptime) IS NOT NULL AND
      DATE(COALESCE(firstupdatedpickeduptime, secondupdatedpickeduptime)) <= DATE(orderdate + INTERVAL '1' DAY)
    )
  ) THEN 1

  -- Các trường hợp còn lại
  ELSE 0

      END AS IsOntime,
      firstupdatedpickeduptime,
      secondupdatedpickeduptime,
      firstfailpicknote,
      lastfailpicknote,
      endpicktime,
      firstcreatedpickeduptime
    FROM "ghn-reporting"."ka"."dtm_ka_v3_createddate" C
    WHERE C.clientid IN (18692)
      AND C.isexpecteddropoff = FALSE
      AND NOT C.channel = 'WH - Shopee'
      AND DATE(C.orderdate) BETWEEN CURRENT_DATE - INTERVAL '14' DAY AND CURRENT_DATE - INTERVAL '1' DAY
      AND c.currentstatus != 'cancel'
  ),
  
  Pending_pick as (
    select 
      date(C.orderdate) AS Time,
      C.ordercode,
      C.currentstatus as Status
      FROM "ghn-reporting"."ka"."dtm_ka_v3_createddate" C
    WHERE C.clientid IN (18692)
      AND C.isexpecteddropoff = FALSE
      AND NOT C.channel = 'WH - Shopee'
      AND DATE(C.orderdate) BETWEEN CURRENT_DATE - INTERVAL '14' DAY AND CURRENT_DATE - INTERVAL '1' DAY
      AND c.currentstatus in ('ready_to_pick','picking')
  )
  
SELECT
  D.Time,
  D.Province,
  D.District,
  W.ward_name as Ward,
  D.Hub,
  D.TypeKH,
  --IsOntime,
  --firstupdatedpickeduptime,
  --secondupdatedpickeduptime,
  --firstfailpicknote,
  --lastfailpicknote,
  --endpicktime,  
  count(p.Status) as Pending_pick,
  COUNT(d.ordercode) AS TotalOrders,
  SUM(d.IsOntime) AS OntimeOrders,
  COUNT(d.ordercode) - SUM(d.IsOntime) AS LateOrders
FROM Details D 
left join Pending_pick P on D.ordercode = P.ordercode
LEFT JOIN "dw-ghn"."datawarehouse"."dim_location_ward" W on w.ward_id = D.Ward_id
WHERE 
Province in ('Hà Nội','Hồ Chí Minh') and 
--District in ('Quận Long Biên') and
   D.Time BETWEEN CURRENT_DATE - INTERVAL '1' DAY AND CURRENT_DATE -- INTERVAL '1' DAY
GROUP BY 1,2,3,4,5,6

----------------------------------------------------------------------check raw pending-----------------------------------------------------------------
WITH
  Details AS (
    SELECT
      date(C.orderdate) AS Time,
      C.ordercode,
      c.fromwardcode AS Ward_id,
      c.fromprovince AS Province,
      c.fromdistrict as District,
      w.ward_name as Ward,
      c.pickwh as Hub,
      c.currentstatus as currentstatus,
      c.firstfailpicknote as firstfailpicknote,
      c.clientcontactname,
      c.clienttype,
      case when
      LOWER(c.pickwh) LIKE 'bưu cục%' then 'Vùng' else 'KHL/GXT'end as TypeKH,
CASE
  -- Ưu tiên 1: endpicktime <= orderdate (Thành công ngay trong ngày D)
  WHEN endpicktime IS NOT NULL AND DATE(endpicktime) <= DATE(orderdate) THEN 'Ontime'

  -- Ưu tiên 2: Giờ đặt < 18h (Deadline: cuối ngày D)
  WHEN HOUR(orderdate) < 18 AND (
    (
      COALESCE(firstfailpicknote, '') = 'Nhân viên gặp sự cố' AND
      secondupdatedpickeduptime IS NOT NULL AND DATE(secondupdatedpickeduptime) <= DATE(orderdate)
    )
    OR (
      COALESCE(firstfailpicknote, '') != 'Nhân viên gặp sự cố' AND
      COALESCE(firstupdatedpickeduptime, secondupdatedpickeduptime) IS NOT NULL AND
      DATE(COALESCE(firstupdatedpickeduptime, secondupdatedpickeduptime)) <= DATE(orderdate)
    )
  ) THEN 'Ontime'

  -- Ưu tiên 3: Giờ đặt >= 18h (Deadline: cuối ngày D + 1)
  WHEN HOUR(orderdate) >= 18 AND (
    (
      COALESCE(firstfailpicknote, '') = 'Nhân viên gặp sự cố' AND
      secondupdatedpickeduptime IS NOT NULL AND DATE(secondupdatedpickeduptime) <= DATE(orderdate + INTERVAL '1' DAY)
    )
    OR (
      COALESCE(firstfailpicknote, '') != 'Nhân viên gặp sự cố' AND
      COALESCE(firstupdatedpickeduptime, secondupdatedpickeduptime) IS NOT NULL AND
      DATE(COALESCE(firstupdatedpickeduptime, secondupdatedpickeduptime)) <= DATE(orderdate + INTERVAL '1' DAY)
    )
  ) THEN 'Ontime'

  -- Các trường hợp còn lại
  ELSE 'Late'

      END AS IsOntime,
      firstupdatedpickeduptime,
      secondupdatedpickeduptime,
      lastfailpicknote,
      endpicktime,
      firstcreatedpickeduptime
    FROM "ghn-reporting"."ka"."dtm_ka_v3_createddate" C
    LEFT JOIN "dw-ghn"."datawarehouse"."dim_location_ward" W on c.fromwardcode = w.Ward_id
    WHERE C.clientid IN (18692)
      AND C.isexpecteddropoff = FALSE
      AND NOT C.channel = 'WH - Shopee'
      --AND DATE(C.orderdate) BETWEEN CURRENT_DATE - INTERVAL '15' DAY AND CURRENT_DATE - INTERVAL '1' DAY
      AND c.currentstatus != 'cancel'
  )
  
SELECT
  D.Time,
  D.ordercode,
  D.TypeKH,
  D.Province,
  D.District,
  D.Ward,
  D.Hub,
  D.currentstatus,
  D.clientcontactname,
  D.clienttype,
  D.firstupdatedpickeduptime,
  D.firstfailpicknote,
  D.endpicktime,
  D.firstcreatedpickeduptime,
  D.IsOntime
FROM Details D
WHERE 
  D.Province in ('Hà Nội','Hồ Chí Minh')
  and D.currentstatus in ('ready_to_pick','picking')
  --AND D.District = 'Quận Long Biên'
  --and D.firstfailpicknote IS NOT NULL
  --AND D.firstfailpicknote != ''
  --AND D.firstfailpicknote NOT IN ('Nhân viên gặp sự cố')

