with sls_tracking as
(
select 
   distinct
   slo_id log_id,
   min(case when tracking_code = 'F000' then from_unixtime(actual_time-3600) else null end) as sls_received_time,
   min(case when tracking_code in ('F100','F510','F420','F450') then from_unixtime(actual_time-3600) else null end) as pku_done_tracking_time,
   min(case when tracking_code = 'F980' then from_unixtime(actual_time-3600) else null end) as dli_done_tracking_time,
   min(case when tracking_code = 'F650' then from_unixtime(actual_time-3600) else null end) as dli_pending_tracking_time,
   min(case when tracking_code = 'F600' then from_unixtime(actual_time-3600) else null end) as dli_delivering_tracking_time,
   min(case when tracking_code in ('F668','F680') then from_unixtime(actual_time-3600) else null end) as return_ini_tracking_time,
   min(case when tracking_code in ('F999','F998') then from_unixtime(actual_time-3600) else null end) as return_done_tracking_time,
   min(case when tracking_code = 'F699' then from_unixtime(actual_time-3600) else null end) as return_pending_tracking_time   

    from sls_mart.shopee_ssc_lts_tracking_vn_db__logistic_tracking_tab__reg_continuous_s0_live
    group by 1
)

, lm as
(
    select 
    slo_id as log_id, 
    substr(description,20) as lm_hub,
    min(from_unixtime(actual_time-3600)) as lm_hub_ib_time
    from sls_mart.shopee_ssc_lts_tracking_vn_db__logistic_tracking_tab__reg_continuous_s0_live
    where tracking_code in ('F599') 
    group by 1,2
)

,b as

(SELECT 
distinct
r.lm_tracking_no 
,o.fulfilment_shipping_carrier
,o.buyer_shipping_address_state
,o.buyer_shipping_address_city
,o.buyer_shipping_address_district
,lm.lm_hub
,lm.lm_hub_ib_time
,sls.pku_done_tracking_time
,coalesce(sls.dli_pending_tracking_time,sls.dli_done_tracking_time,sls.return_pending_tracking_time,sls.return_done_tracking_time) as first_delivery_attempt
,sls.dli_done_tracking_time
, case when date_diff('day',sls.pku_done_tracking_time,current_date) <= 6 then 1 else 0 end as Aging_6_days
, case when date_diff('day',sls.pku_done_tracking_time,current_date) > 6 and date_diff('day',sls.pku_done_tracking_time,current_date) <= 11 then 1 else 0 end as Aging_6_11_days
, case when date_diff('day',sls.pku_done_tracking_time,current_date) > 11 then 1 else 0 end as Aging_12_days



FROM (SELECT * from mp_order.dwd_order_all_ent_df__vn_s0_live where grass_date > date '2022-08-01') o
LEFT JOIN (SELECT * FROM sls_mart.dwd_parcel_detail_union_nonsensitive_df_vn where grass_date = current_date - interval '1' day) r on o.order_sn = array_join(ordersn_list,', ')
LEFT JOIN (SELECT * FROM sls_mart.dwd_parcel_detail_union_df_vn where grass_date = current_date - interval '1' day) rd on rd.log_id = r.log_id
LEFT JOIN sls_tracking sls on sls.log_id = r.log_id
LEFT JOIN lm on lm.log_id = r.log_id

where 1=1 
--and lm.lm_hub_ib_time is not null
and sls.pku_done_tracking_time is not null
and sls.dli_done_tracking_time is null 
and sls.return_done_tracking_time is null
and o.logistics_status_id <> 11 
and o.fulfilment_shipping_carrier in ('VNPost Nhanh','VNPost Tiết Kiệm')
and date(from_unixtime(o.create_timestamp-3600)) > date '2022-09-01'
)

select 
distinct

lm_tracking_no
buyer_shipping_address_state,
buyer_shipping_address_city,
buyer_shipping_address_district,
Aging_6_days,
Aging_6_11_days,
Aging_12_days
--count(distinct lm_tracking_no) as total_backlog

from b 
--and lm_hub in (' 21-HNI Dong Anh LM Hub')
--GROUP BY 1,2,3
--order by total_backlog desc 


