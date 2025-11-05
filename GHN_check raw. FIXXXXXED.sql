WITH
  Details AS (
    SELECT
      DATE(C.orderdate) AS Time --thay đổi sang Week/Month
      ,C.ordercode ordercode
      ,c.fromwardcode AS Ward_id
      ,c.fromprovince as Province
      --,COUNT(C.ordercode) AS Volume_Created
      --,SUM(
        ,CASE
  -- Ưu tiên 1: endpicktime <= orderdate
  WHEN endpicktime IS NOT NULL AND DATE(endpicktime) <= DATE(orderdate) THEN 'Ontime'

  -- Ưu tiên 2: firstupdatedpickeduptime <= orderdate và không phải lỗi 'Nhân viên gặp sự cố'
  WHEN firstupdatedpickeduptime IS NOT NULL 
       AND DATE(firstupdatedpickeduptime) <= DATE(orderdate)
       AND COALESCE(firstfailpicknote, '') != 'Nhân viên gặp sự cố'
  THEN 'Ontime'

  -- Ưu tiên 3: Giờ đặt < 18h và ngày lấy hàng <= ngày đặt
  WHEN HOUR(orderdate) < 18 
       AND DATE(COALESCE(firstupdatedpickeduptime, endpicktime)) <= DATE(orderdate)
       AND COALESCE(firstfailpicknote, '') != 'Nhân viên gặp sự cố'
  THEN 'Ontime'

  -- Ưu tiên 4: Giờ đặt >= 18h và ngày lấy hàng <= ngày đặt + 1
  WHEN HOUR(orderdate) >= 18 
       AND DATE(COALESCE(firstupdatedpickeduptime, endpicktime)) <= DATE(orderdate + INTERVAL '1' DAY)
       AND COALESCE(firstfailpicknote, '') != 'Nhân viên gặp sự cố'
  THEN 'Ontime'

  -- Các trường hợp còn lại
  ELSE 'Late'
END AS OntimeFirstPUcheck



       ,firstupdatedpickeduptime
       ,secondupdatedpickeduptime
       ,firstfailpicknote
       ,lastfailpicknote
       ,endpicktime
       ,firstcreatedpickeduptime
    FROM "ghn-reporting"."ka"."dtm_ka_v3_createddate" C
    WHERE C.clientid IN (18692)
      AND C.isexpecteddropoff = FALSE
      AND NOT C.channel = 'WH - Shopee'
      AND DATE(C.orderdate) BETWEEN CURRENT_DATE - INTERVAL '14' DAY AND CURRENT_DATE - INTERVAL '1' DAY
      --AND C.fromregionshortname = 'HNO'
      --AND LOWER(pickwh) LIKE 'bưu cục%'
    --GROUP BY 1, 2
  )
  SELECT
    Time
    ,ordercode
    --,Ward_id
    --,ROUND((AVG(OntimeFirstPU) / AVG(Volume_Created)),4) AS Ontime
    ,OntimeFirstPUcheck
    ,firstupdatedpickeduptime
    ,secondupdatedpickeduptime
    ,firstfailpicknote
    ,endpicktime
    ,lastfailpicknote
    ,firstcreatedpickeduptime
  FROM Details
  where Province = ('Hà Nội')
  and Time = date('2025-11-03')
  --and ordercode in
