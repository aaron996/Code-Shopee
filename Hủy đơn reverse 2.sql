-- Data raw đơn đủ điều kiện hủy
SELECT
  C.ordercode AS MaDH
  ,S.clientid AS LoaiDonHang
  ,DATE(C.orderdate) AS NgayTaoDon
  ,C.currentstatus AS TrangThai
  ,C.numpick AS SoLanLay
FROM "ghn-reporting"."ka"."dtm_ka_v3_createddate" C
JOIN "ghn-reporting"."ka"."dtm_ka_shopee" AS S
  ON C.ordercode = S.ordercode
WHERE S.clientid IN ('224845 - Reverse API','224845 - Reverse Manual','224845')
  AND C.currentstatus IN ('ready_to_pick','picking')
  AND isexpecteddropoff = false
    AND (C.numpick >= 3 
    AND date_diff('day',DATE(C.orderdate),CURRENT_DATE) > 6)
  OR
    (AND C.numpick < 3
    AND date_diff('day',DATE(C.orderdate),CURRENT_DATE) > 10)
ORDER BY 3,2

-- Data raw đơn đủ điều kiện hủy
SELECT
  C.ordercode AS MaDH
  ,S.clientid AS LoaiDonHang
  ,DATE(C.orderdate) AS NgayTaoDon
  ,C.currentstatus AS TrangThai
  ,C.numpick AS SoLanLay
FROM "ghn-reporting"."ka"."dtm_ka_v3_createddate" C
JOIN "ghn-reporting"."ka"."dtm_ka_shopee" AS S
  ON C.ordercode = S.ordercode
WHERE S.clientid IN ('224845 - Reverse API','224845 - Reverse Manual','224845')
  AND C.numpick < 3
  AND date_diff('day',DATE(C.orderdate),CURRENT_DATE) > 10
  AND C.currentstatus IN ('ready_to_pick','picking')
  AND isexpecteddropoff = false
ORDER BY 3,2
