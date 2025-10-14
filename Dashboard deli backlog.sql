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


,b as

(SELECT 
distinct
r.lm_tracking_no
,o.fulfilment_shipping_carrier
,o.buyer_shipping_address_state
,o.buyer_shipping_address_city
,sls.pku_done_tracking_time
,coalesce(sls.dli_pending_tracking_time,sls.dli_done_tracking_time,sls.return_pending_tracking_time,sls.return_done_tracking_time) as first_delivery_attempt
,sls.dli_done_tracking_time
,o.logistics_status 
, case when date_diff('day',sls.pku_done_tracking_time,current_date) < 5 then 1 else 0 end as Aging_5_days
, case when date_diff('day',sls.pku_done_tracking_time,current_date) >= 5 and date_diff('day',sls.pku_done_tracking_time,current_date) <= 7 then 1 else 0 end as Aging_5_7_days
, case when date_diff('day',sls.pku_done_tracking_time,current_date) > 7 then 1 else 0 end as Aging_7_days




FROM (SELECT * from mp_order.dwd_order_all_ent_df__vn_s0_live where grass_date > date '2022-08-01') o
LEFT JOIN (SELECT * FROM sls_mart.dwd_parcel_detail_union_nonsensitive_df_vn where grass_date = current_date - interval '1' day) r on o.order_sn = array_join(ordersn_list,', ')
-- LEFT JOIN (SELECT * FROM sls_mart.dwd_parcel_detail_union_df_vn where grass_date = current_date - interval '1' day) rd on rd.log_id = r.log_id
LEFT JOIN sls_tracking sls on sls.log_id = r.log_id

where 1=1 
and sls.pku_done_tracking_time is not null
and sls.dli_done_tracking_time is null 
and sls.return_done_tracking_time is null
and o.logistics_status_id <> 11 
and o.fulfilment_shipping_carrier in ('Ninja Van')
and date(pku_done_tracking_time) BETWEEN current_date - interval '10' day and current_date 
and r.is_reverse = 0
and o.logistics_status = 'PICKUP DONE'
)

select 
distinct
buyer_shipping_address_state as buyer_states,
buyer_shipping_address_city as buyer_city,
sum(Aging_5_days) Aging_5_days,
sum(Aging_5_7_days) Aging_5_7_days,
sum(Aging_7_days) Aging_7_days,
count(distinct lm_tracking_no) as total_backlog

from b 
GROUP BY 1,2
order by total_backlog desc 

