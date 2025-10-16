
--code lấy all đơn
SELECT
  ordercode
  ,clientid
  ,DATE(enddeliverytime) AS "enddeliverytime"
  ,DATE(lastdeliveredupdatedtime) AS "lastdeliveredupdatedtime"
  ,currentstatus AS Status
  ,numdeliver
  ,numreturn
  ,currentwh
FROM dtm_ka_v3_createddate
WHERE clientid = 224845
  AND numdeliver >= 1
  AND towardcode = '21903'
  AND returnwardcode = '21903'
  AND MONTH(orderdate) IN (7,8,9,10)
  AND deliverywarehouseid = 1327

-- code chạy mã đơn trong email

SELECT 
  GS."OrderCode"
  ,clientid
  ,DATE(enddeliverytime) AS "enddeliverytime"
  ,DATE(lastdeliveredupdatedtime) AS "lastdeliveredupdatedtime"
  ,currentstatus AS Status
  ,numdeliver
  ,numreturn
  ,currentwh
FROM "gsheet-data_input_from_external"."default"."input_customer_shopee" GS
LEFT JOIN "ghn-reporting"."ka".dtm_ka_v3_createddate C
  ON GS."OrderCode" = C.ordercode
WHERE GS."OrderCode" IS NOT NULL

