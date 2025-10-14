WITH
--multi AS
--(
   -- Select
    --log_id,
    --count(distinct lm_tracking_no) as nb_parcels
   -- FROM (select * from sls_mart.dwd_parcel_detail_union_nonsensitive_df_vn where grass_date > date '2021-01-01')
    --where whs_id is not null
    --GROUP by 1
  --  HAVING count(distinct lm_tracking_no) > 1
--)
--,group_shipment AS
--(
--SELECT
  --  order_id,
    --count(oms_forder_id) As total_group
    
--FROM
  --   (select * from sls_mart.dwd_ofg_forder_union_df_vn  where grass_date = current_date - interval '1' day)
    
--GROUP by 1
--)
--,pku_failed as
--(
  --  select slo_id, min(from_unixtime(actual_time-3600)) as tracking_time
    --from sls_mart.shopee_ssc_lts_tracking_vn_db__logistic_tracking_tab__reg_continuous_s0_live
    --where tracking_code in ('F097')
    --group by 1
--)
lost_time as
(
    select orderid,
max(from_unixtime(ctime-3600)) lost_update_time 
from marketplace.shopee_logistics_audit_v3_db__logistics_audit_tab__vn_continuous_s0_live 
group by 1 
)
,sls_tracking AS (
   SELECT
    slo_id log_id
   , min(case when tracking_code in ('F100','F510') then from_unixtime((CASE WHEN (ctime > actual_time) THEN (actual_time - 3600) ELSE (ctime - 3600) END)) end ) pku_done_time
   , min(case when tracking_code ='F980' then from_unixtime((CASE WHEN (ctime > actual_time) THEN (actual_time - 3600) ELSE (ctime - 3600) END)) end)  deli_done_time
   , min(case when tracking_code in ('F600','F650','F980','F668','F680','F999') then from_unixtime((CASE WHEN (ctime > actual_time) THEN (actual_time - 3600) ELSE (ctime - 3600) END)) end ) deli_1st_time
   , min(case when tracking_code ='F668' then from_unixtime((CASE WHEN (ctime > actual_time) THEN (actual_time - 3600) ELSE (ctime - 3600) END)) end) return_initiated_time
   , min(case when tracking_code ='F999' then from_unixtime((CASE WHEN (ctime > actual_time) THEN (actual_time - 3600) ELSE (ctime - 3600) END)) end)  deli_failed_time
   FROM
    sls_mart.shopee_ssc_lts_tracking_vn_db__logistic_tracking_tab__reg_continuous_s0_live 
   GROUP BY 1
)
SELECT distinct
r.lm_tracking_number
,o.order_id
,o.order_sn
,r.slo_tn consignment_no
,Case when o.logistics_status_id = 0 then 'LOGISTICS_NOT_STARTED'
        when o.logistics_status_id = 1 then 'LOGISTICS_REQUEST_CREATED'
        when o.logistics_status_id = 2 then 'LOGISTICS_PICKUP_DONE'
        when o.logistics_status_id = 3 then 'LOGISTICS_PICKUP_RETRY'
        when o.logistics_status_id = 4 then 'LOGISTICS_PICKUP_FAILED'
        when o.logistics_status_id = 5 then 'LOGISTICS_DELIVERY_DONE'
        when o.logistics_status_id = 6 then 'LOGISTICS_DELIVERY_FAILED'
        when o.logistics_status_id = 7 then 'LOGISTICS_REQUEST_CANCELED'
        when o.logistics_status_id = 8 then 'LOGISTICS_COD_REJECTED'
        when o.logistics_status_id= 9 then 'LOGISTICS_READY'
        when o.logistics_status_id = 10 then 'LOGISTICS_INVALID'
        when o.logistics_status_id = 11 then 'LOGISTICS_LOST'
        when o.logistics_status_id = 12 then 'LOGISTICS_PENDING_ARRANGE' END AS Logistics_Status
,o.order_be_status
, case
    when r.slo_status = 1000 then 'NEW'
    when r.slo_status in (1001,1040) then 'INFO_RECEIVED'
    when r.slo_status in (1061,1051,2001,2008,2009,2015,2030,2040,3001,3060,3080,3090) then 'IN_TRANSIT'
    when r.slo_status in (9997,3099,3070) then 'EXCEPTION'
    when r.slo_status = 9999 then 'CANCELLED'
    when r.slo_status = 1050 then 'DOMESTIC_PICKUP'
    when r.slo_status in  (4010,4020,4100) then 'DOMESTIC_SORTING'
    when r.slo_status = 4060 then 'DOMESTIC_DELIVERING'
    when r.slo_status = 4097 then 'DOMESTIC_DELIVERED'
    when r.slo_status = 4070 then 'DELIVERY_PENDING'
    when r.slo_status = 9998 then 'PARCEL_LOST'
    when r.slo_status =  8020 then 'PARCEL_RETURN'
    when r.slo_status = 8050 then 'RETURN_COMPLETED'
    when r.slo_status = 8099 then 'RETURN_FAILED'
    when r.slo_status = 8045 then 'PENDING_FOR_RETURN'
    when r.slo_status in (4098,8001,4102) then 'REQUEST_RETURN'
    when r.slo_status in (996,1041,8010) then 'PICKUP_RETRY'
    when r.slo_status in (1099,2099,9995) then 'DOMESTIC_PICKUP_FAILED'
    when r.slo_status = 1060 then 'DOMESTIC_DROPOFF'
    when r.slo_status = 1042 then 'DOMESTIC_PICKUP_PENDING'
    when r.slo_status in (8000,8040) then 'RETURN_STARTED'
    when r.slo_status = 4101 then 'UPDATE_STORE_SUBMITTED'
    when r.slo_status = 4103 then 'UPDATE_STORE_CANCEL'
    when r.slo_status = 9994 then 'IMLM_DISPOSED'
    when r.slo_status = 8060 then 'PARCEL_CONFISCATED'
    when r.slo_status = 9993 then 'TERMINATE_AS_DAMAGE'
    else 'no'
  end as sls_status
,o.seller_shipping_address_state as seller_state
,o.seller_shipping_address_city AS seller_city
,o.seller_shipping_address_district AS seller_district
,o.buyer_shipping_address_state as buyer_state
,o.buyer_shipping_address_city as buyer_city
,o.buyer_shipping_address_district as buyer_district
--,o.seller_id
--,o.shop_id
--,case when o.is_preferred_shop = 1 then 'preferred_shop' 
--when o.is_preferred_plus_shop = 1 then 'preferred_plus_shop' 
--when o.is_official_shop = 1 then 'shop Mall' 
--else 'no' 
--end as seller_type
--,o.shop_name
, case          
when o.fulfilment_channel_id  =    50022   then    'Shopee Express Instant'
when o.fulfilment_channel_id  =    50020   then    'GrabExpress'
when o.fulfilment_channel_id  =    50026   then    'beDelivery'
when o.fulfilment_channel_id  =    5000    then    'Hỏa Tốc'
when o.fulfilment_channel_id  =    50024   then    'BEST Express'
when o.fulfilment_channel_id  =    50021   then    'Shopee Express'
when o.fulfilment_channel_id  =    5001    then    'Nhanh'
when o.fulfilment_channel_id  =    50018   then    'J&T Express'
when o.fulfilment_channel_id  =    50025   then    'Đợi phân bổ ĐVVC'
when o.fulfilment_channel_id  =    50010   then    'Viettel Post'
-- when t2.fulfilment_channel_id  =    50011   then    'Giao Hàng Nhanh'
when o.fulfilment_channel_id  =    50012   then    'Giao Hàng Tiết Kiệm'
when o.fulfilment_channel_id  =    50015   then    'VNPost Nhanh'
when o.fulfilment_channel_id  =    50023   then    'Ninja Van'
when o.fulfilment_channel_id  =    50016   then    'VNPost Tiết Kiệm'
when o.fulfilment_channel_id  =    5002    then    'Tiết kiệm'
when o.fulfilment_channel_id = 50011 and o.shipping_method_id  in (5001)  then 'Giao Hàng Nhanh'
-- when t2.fulfilment_channel_id = 50011 and l.masking_product_id  in ('5002')  then 'Giao Hàng Nhanh - ECO'
when o.fulfilment_channel_id = 50011 and o.shipping_method_id  in (5002)  then 'Giao Hàng Nhanh - ECO'
when o.fulfilment_shipping_carrier like '%Standard Express%' and t3.lm_shipment_provider like '%SPX%' then 'Shopee Express - CB'
when o.fulfilment_shipping_carrier like '%Standard Express%' and regexp_like(upper(t3.lm_shipment_provider), 'JNT|J&T') then 'J&T Express - CB'
else  o.fulfilment_shipping_carrier end "3PL name"
--,case when multi.nb_parcels IS NOT NULL then 'Multi WH' else 'Not Multi WH' end as "Check_Multi_WH"
--,o.escrow_to_seller_amt escrow_to_seller
--,o.gmv grand_total
--,t1.forder_value_local grand_total_parcel
--,t1.items_sales_value_local cogs_parcel
--,r.charged_weight parcel_chargeable_weight
--,t3.parcel_chargeable_weight parcel_actual_weight
--,from_unixtime(o.create_timestamp - 3600) Order_created
,from_unixtime(t1.order_arrange_shipping_timestamp - 3600) Logistics_request_created
--,o.shipping_confirm_datetime
,sls.pku_done_time pickup_done_time
--,sls.deli_done_time
--,t3.fm_pickup_failed_datetime pickup_failed
,sls.return_initiated_time return_initiated
--,sls.deli_failed_time
,(case when o.payment_method = 'PAY_COD' then 'COD' else 'CC' end) as payment_method
,case when t3.inbound_type = 1 then 'pickup' when t3.inbound_type = 2 then 'dropoff' else null end as shipmet_type
,from_unixtime(t3.scheduled_pickup_timestamp-3600) scheduled_pickup_time
,o.auto_cancel_arrange_ship_datetime ACL1_date
,date(from_unixtime(o.auto_cancel_3pl_ack_timestamp - 3600)) ACL2_date
,o.cancel_datetime cancel_timestamp
, o.escrow_paid_datetime
, o.escrow_created_datetime
, o.escrow_to_seller_amt
--,case when gs.total_group > 1 then 'Grouped' else 'Not grouped' end as Check_group_shipment

FROM (SELECT * from mp_order.dwd_order_all_ent_df__vn_s0_live where grass_date >= date'2022-01-01' and grass_region = 'VN') o
LEFT JOIN sls_mart.shopee_ssc_lfs_order_vn_db__logistic_order_tab__vn_continuous_s0_live r on o.order_sn = r.ordersn
LEFT JOIN (SELECT * FROM sls_mart.dwd_ofg_forder_union_df_vn where grass_date = current_date - interval '1' day and grass_region = 'VN') t1 on t1.log_id = r.slo_id
left join lost_time l on o.order_id = l.orderid 
--LEFT JOIN return ri on ri.slo_id = r.slo_id
LEFT JOIN (SELECT * FROM sls_mart.dwd_parcel_detail_union_nonsensitive_df_vn where grass_date = current_date - interval '1' day and grass_region = 'VN') t3 on t3.log_id = r.slo_id
--left join multi on multi.log_id = r.slo_id
--Left join group_shipment on group_shipment.order_id = o.order_id
left join sls_tracking sls on sls.log_id = r.slo_id


where 
o.cancel_datetime is not null 
and o.fulfilment_channel_id in (50015,50016)
and sls.pku_done_time is not null 
and to_unixtime(sls.pku_done_time) > to_unixtime(o.cancel_datetime)
and date(o.cancel_datetime) between date '2023-02-15' and current_date 
