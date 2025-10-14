with pku_done as
(
    select slo_id as log_id, min(from_unixtime(actual_time-3600)) as tracking_time
    from sls_mart.shopee_ssc_lts_tracking_vn_db__logistic_tracking_tab__reg_continuous_s0_live
    where tracking_code in  ('F100')
    --and grass_date = current_date - interval '1' day
    --and grass_region ='VN'
    group by 1
)
, dli_done as
(
    select slo_id as log_id, min(from_unixtime(actual_time-3600)) as tracking_time
    from sls_mart.shopee_ssc_lts_tracking_vn_db__logistic_tracking_tab__reg_continuous_s0_live
    where tracking_code in ('F980') 
    --and grass_date = current_date - interval '1' day
    --and grass_region ='VN'
    group by 1
)

, dli_failed as
(
    select slo_id as log_id, min(from_unixtime(actual_time-3600)) as tracking_time
    from sls_mart.shopee_ssc_lts_tracking_vn_db__logistic_tracking_tab__reg_continuous_s0_live
    where tracking_code in ('F999') 
    --and grass_date = current_date - interval '1' day
    --and grass_region ='VN'
    group by 1
)

,st_pku_1 as
(select * from (
select distinct a.slo_id, a.reason_description tracking_detail_reason, 
from_unixtime(a.actual_time-3600) tracking_time, 
rank() over ( partition by a.slo_id order by actual_time asc) rnk
from sls_mart.shopee_ssc_lts_tracking_vn_db__logistic_tracking_tab__reg_continuous_s0_live a
where date(from_unixtime(a.actual_time-3600)) >= date '2022-08-31' and tracking_code = 'F001'
) where rnk = 1
)

,st_pku_2 as
(select * from (
select distinct a.slo_id, a.reason_description tracking_detail_reason, 
from_unixtime(a.actual_time-3600) tracking_time, 
rank() over ( partition by a.slo_id order by actual_time asc) rnk
from sls_mart.shopee_ssc_lts_tracking_vn_db__logistic_tracking_tab__reg_continuous_s0_live a
where date(from_unixtime(a.actual_time-3600)) >= date '2022-08-31' and tracking_code = 'F001'
) where rnk = 2
)

,st_pku_3 as
(select * from (
select distinct a.slo_id, a.reason_description tracking_detail_reason, 
from_unixtime(a.actual_time-3600) tracking_time, 
rank() over ( partition by a.slo_id order by actual_time asc) rnk
from sls_mart.shopee_ssc_lts_tracking_vn_db__logistic_tracking_tab__reg_continuous_s0_live a
where date(from_unixtime(a.actual_time-3600)) >= date '2022-08-31' and tracking_code = 'F001'
) where rnk = 3
)


,st_dli_1 as
(select * from (
select distinct a.slo_id, a.reason_description tracking_detail_reason, 
from_unixtime(a.actual_time-3600) tracking_time, 
rank() over ( partition by a.slo_id order by actual_time asc) rnk
from sls_mart.shopee_ssc_lts_tracking_vn_db__logistic_tracking_tab__reg_continuous_s0_live a
where date(from_unixtime(a.actual_time-3600)) >= date '2022-08-31' and tracking_code = 'F650'
) where rnk = 1
)

,st_dli_2 as
(select * from (
select distinct a.slo_id, a.reason_description tracking_detail_reason, 
from_unixtime(a.actual_time-3600) tracking_time, 
rank() over ( partition by a.slo_id order by actual_time asc) rnk
from sls_mart.shopee_ssc_lts_tracking_vn_db__logistic_tracking_tab__reg_continuous_s0_live a
where date(from_unixtime(a.actual_time-3600)) >= date '2022-08-31' and tracking_code = 'F650'
) where rnk = 2
)

,st_dli_3 as
(select * from (
select distinct a.slo_id, a.reason_description tracking_detail_reason, 
from_unixtime(a.actual_time-3600) tracking_time, 
rank() over ( partition by a.slo_id order by actual_time asc) rnk
from sls_mart.shopee_ssc_lts_tracking_vn_db__logistic_tracking_tab__reg_continuous_s0_live a
where date(from_unixtime(a.actual_time-3600)) >= date '2022-08-31' and tracking_code = 'F650'
) where rnk = 3
)
,return as
(select * from (
select distinct a.slo_id, a.reason_description tracking_detail_reason, 
from_unixtime(a.actual_time-3600) tracking_time, 
rank() over ( partition by a.slo_id order by actual_time asc) rnk
from sls_mart.shopee_ssc_lts_tracking_vn_db__logistic_tracking_tab__reg_continuous_s0_live a
where date(from_unixtime(a.actual_time-3600)) >= date '2022-08-31' and tracking_code = 'F668'
) where rnk = 1
)

SELECT distinct
o.order_sn
--,o.order_id
,r.lm_tracking_no
,r.consignment_no
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
,from_unixtime(o.create_timestamp-3600) created_datetime 
,o.seller_shipping_address_state as "Tỉnh lấy"
,o.seller_shipping_address_city AS "Quận huyện lấy"
,o.seller_shipping_address_district AS "Phường xã lấy"
,from_unixtime(r.scheduled_pickup_timestamp-3600) as "Ngày hẹn thu gom"
--,o.buyer_shipping_address_state as buyer_state
--,o.buyer_shipping_address_city as buyer_city
--,o.buyer_shipping_address_district as buyer_district
,sp1.tracking_time as "1st_pickup_attempt"
,sp2.tracking_time as "2nd_pickup_attempt"
,sp3.tracking_time as "3rd_pickup_attempt"
,sp1.tracking_detail_reason as "1st_pickup_reason"
,sp2.tracking_detail_reason as "2nd_pickup_reason"
,sp3.tracking_detail_reason as "3rd_pickup_reason"
--,pd.tracking_time as pickup_done
--,sd1.tracking_time as "1st_dli_attempt"
--,sd2.tracking_time as "2nd_dli_attempt"
--,sd3.tracking_time as "3rd_dli_attempt"
--,ri.tracking_time as "chuyển hoàn"
--,lower(sd1.tracking_detail_reason) as "1st_dli_reason"
--,lower(sd2.tracking_detail_reason) as "2nd_dli_reason"
--,lower(sd3.tracking_detail_reason) as "3rd_dli_reason"
--,dd.tracking_time as delivery_done
--,df.tracking_time as returnedtosellertimestamp
--,o.cancel_reason
--,o.buyer_cancel_reason

FROM (SELECT * from mp_order.hourly_dwd_order_item_subset_all_ent_hf__vn_s0_live where grass_date > date '2022-08-01' and grass_region = 'VN') o
LEFT JOIN (SELECT * FROM sls_mart.dwd_parcel_detail_union_nonsensitive_df_vn where grass_date = current_date - interval '1' day) r on o.order_sn = array_join(ordersn_list,', ')
LEFT JOIN (SELECT * FROM sls_mart.dwd_parcel_detail_union_df_vn where grass_date = current_date - interval '1' day) rd on rd.log_id = r.log_id
--LEFT JOIN location on location.order_sn = o.order_sn
LEFT JOIN pku_done pd on pd.log_id = r.log_id
LEFT JOIN dli_done dd on dd.log_id = r.log_id
LEFT JOIN dli_failed df on df.log_id = r.log_id
LEFT JOIN st_pku_1 sp1 on sp1.slo_id = r.log_id
LEFT JOIN st_pku_2 sp2 on sp2.slo_id = r.log_id
LEFT JOIN st_pku_3 sp3 on sp3.slo_id = r.log_id
LEFT JOIN st_dli_1 sd1 on sd1.slo_id = r.log_id
LEFT JOIN st_dli_2 sd2 on sd2.slo_id = r.log_id
LEFT JOIN st_dli_3 sd3 on sd3.slo_id = r.log_id
LEFT JOIN return ri on ri.slo_id = r.log_id


--where date(from_unixtime(o.create_timestamp-3600)) between date '2022-04-01' and  date '2022-04-15'


WHERE 
--date(from_unixtime(o.auto_cancel_3pl_ack_timestamp - 3600)) between date '2022-10-24' and date '2022-10-25'
date(from_unixtime(r.scheduled_pickup_timestamp-3600)) <= (current_date + interval '1' day)
and year(from_unixtime(r.scheduled_pickup_timestamp-3600)) <> 1970
and o.fulfilment_shipping_carrier in ('VNPost Nhanh','VNPost Tiết Kiệm')
and o.logistics_status_id = 1
and (
    sp1.tracking_time is null 
    or sp2.tracking_time is null 
    or sp3.tracking_time is null
)
--limit 100
--r.lm_tracking_no in ()
