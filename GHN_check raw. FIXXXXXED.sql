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
  )
SELECT
  Time,
  Province,
  District,
  Hub,
  TypeKH,
  --IsOntime,
  --firstupdatedpickeduptime,
  --secondupdatedpickeduptime,
  --firstfailpicknote,
  --lastfailpicknote,
  --endpicktime,  
  COUNT(ordercode) AS TotalOrders,
  SUM(IsOntime) AS OntimeOrders,
  COUNT(ordercode) - SUM(IsOntime) AS LateOrders
FROM Details
WHERE 
--Province = 'Hà Nội'
   Time BETWEEN CURRENT_DATE - INTERVAL '17' DAY AND CURRENT_DATE - INTERVAL '1' DAY
GROUP BY 1,2,3,4,5
