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
where date(from_unixtime(a.actual_time-3600)) >= date '2022-01-01' and tracking_code = 'F001'
) where rnk = 1
)

,st_pku_2 as
(select * from (
select distinct a.slo_id, a.reason_description tracking_detail_reason, 
from_unixtime(a.actual_time-3600) tracking_time, 
rank() over ( partition by a.slo_id order by actual_time asc) rnk
from sls_mart.shopee_ssc_lts_tracking_vn_db__logistic_tracking_tab__reg_continuous_s0_live a
where date(from_unixtime(a.actual_time-3600)) >= date '2022-01-01' and tracking_code = 'F001'
) where rnk = 2
)

,st_pku_3 as
(select * from (
select distinct a.slo_id, a.reason_description tracking_detail_reason, 
from_unixtime(a.actual_time-3600) tracking_time, 
rank() over ( partition by a.slo_id order by actual_time asc) rnk
from sls_mart.shopee_ssc_lts_tracking_vn_db__logistic_tracking_tab__reg_continuous_s0_live a
where date(from_unixtime(a.actual_time-3600)) >= date '2022-01-01' and tracking_code = 'F001'
) where rnk = 3
)


,st_dli_1 as
(select * from (
select distinct a.slo_id, a.reason_description tracking_detail_reason, 
from_unixtime(a.actual_time-3600) tracking_time, 
rank() over ( partition by a.slo_id order by actual_time asc) rnk
from sls_mart.shopee_ssc_lts_tracking_vn_db__logistic_tracking_tab__reg_continuous_s0_live a
where date(from_unixtime(a.actual_time-3600)) >= date '2022-01-01' and tracking_code = 'F650'
) where rnk = 1
)

,st_dli_2 as
(select * from (
select distinct a.slo_id, a.reason_description tracking_detail_reason, 
from_unixtime(a.actual_time-3600) tracking_time, 
rank() over ( partition by a.slo_id order by actual_time asc) rnk
from sls_mart.shopee_ssc_lts_tracking_vn_db__logistic_tracking_tab__reg_continuous_s0_live a
where date(from_unixtime(a.actual_time-3600)) >= date '2022-01-01' and tracking_code = 'F650'
) where rnk = 2
)

,st_dli_3 as
(select * from (
select distinct a.slo_id, a.reason_description tracking_detail_reason, 
from_unixtime(a.actual_time-3600) tracking_time, 
rank() over ( partition by a.slo_id order by actual_time asc) rnk
from sls_mart.shopee_ssc_lts_tracking_vn_db__logistic_tracking_tab__reg_continuous_s0_live a
where date(from_unixtime(a.actual_time-3600)) >= date '2022-01-01' and tracking_code = 'F650'
) where rnk = 3
)
,return as
(select * from (
select distinct a.slo_id, a.reason_description tracking_detail_reason, 
from_unixtime(a.actual_time-3600) tracking_time, 
rank() over ( partition by a.slo_id order by actual_time asc) rnk
from sls_mart.shopee_ssc_lts_tracking_vn_db__logistic_tracking_tab__reg_continuous_s0_live a
where date(from_unixtime(a.actual_time-3600)) >= date '2022-01-01' and tracking_code = 'F668'
) where rnk = 1
)

SELECT distinct
o.order_sn
,o.order_id
,r.lm_tracking_no
,r.consignment_no
,from_unixtime(o.create_timestamp-3600) created_datetime 
--,o.seller_shipping_address_state as seller_state
--,o.seller_shipping_address_city AS seller_city
--,o.seller_shipping_address_district AS seller_district
,o.buyer_shipping_address_state as buyer_state
,o.buyer_shipping_address_city as buyer_city
,o.buyer_shipping_address_district as buyer_district
--,sp1.tracking_time as "1st_pickup_attempt"
--,sp2.tracking_time as "2nd_pickup_attempt"
--,sp3.tracking_time as "3rd_pickup_attempt"
--,sp1.tracking_detail_reason as "1st_pickup_reason"
--,sp2.tracking_detail_reason as "2nd_pickup_reason"
--,sp3.tracking_detail_reason as "3rd_pickup_reason"
,pd.tracking_time as pickup_done
,sd1.tracking_time as "1st_dli_attempt"
,sd2.tracking_time as "2nd_dli_attempt"
,sd3.tracking_time as "3rd_dli_attempt"
,sd1.tracking_detail_reason as "1st_dli_reason"
,sd2.tracking_detail_reason as "2nd_dli_reason"
,sd3.tracking_detail_reason as "3rd_dli_reason"
--,dd.tracking_time as delivery_done
--,df.tracking_time as returnedtosellertimestamp
,case when sd2.tracking_time is null then 1
      when sd3.tracking_time is null then 2
      when sd3.tracking_time is not null then 3
      else 0
      end as "Số ca giao"
,case when (sd1.tracking_time is not null and sd2.tracking_time is not null and (cast(to_unixtime(sd2.tracking_time) as double) - cast(to_unixtime(sd1.tracking_time)as double)) < 900 
        or (sd2.tracking_time is not null and sd3.tracking_time is not null and (cast(to_unixtime(sd3.tracking_time) as double) - cast(to_unixtime(sd2.tracking_time)as double)) < 900)) then 'Gọi 2 lần trong vòng 15 phút'
      when ((sd2.tracking_time is not null and hour(sd1.tracking_time) < 13 and hour(sd2.tracking_time) > 12)
       or (sd3.tracking_time is not null and hour(sd2.tracking_time) < 13 and hour(sd3.tracking_time) > 12)) then '2 ca sáng chiều'
      when ((sd2.tracking_time is not null and to_unixtime(sd2.tracking_time) - to_unixtime(sd1.tracking_time) < 18000)
       or (sd3.tracking_time is not null and to_unixtime(sd3.tracking_time) - to_unixtime(sd2.tracking_time) < 18000)) then 'Không hợp lệ'
      else 'Hợp lệ'
      end as "Thời gian giữa 2 ca giao"
,case when 
(hour(sd1.tracking_time) > 21 
or hour(sd1.tracking_time) < 6 
or hour(sd2.tracking_time) > 21 
or hour(sd2.tracking_time) < 6 
or hour(sd2.tracking_time) > 21 
or hour(sd3.tracking_time) < 6) then 'Bất thường' else 'Bình thường' end as "Thời gian giao hàng"
--,o.cancel_reason
--,o.buyer_cancel_reason

FROM (SELECT * from mp_order.dwd_order_all_ent_df__vn_s0_live where grass_date > date '2022-01-01') o
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
date(df.tracking_time) between date '2023-01-01' and date '2023-01-10'
and o.fulfilment_shipping_carrier in ('VNPost Nhanh','VNPost Tiết Kiệm')
and o.logistics_status_id = 6
and (
    (
        regexp_like(sd1.tracking_detail_reason,'tu choi|thay doi dia chi phat|khong tim thay|sai') = false
        and regexp_like(sd2.tracking_detail_reason,'tu choi|thay doi dia chi phat|khong tim thay|sai') = false
        and regexp_like(sd3.tracking_detail_reason,'tu choi|thay doi dia chi phat|khong tim thay|sai') = false
    )
or (
sd3.tracking_time is null 
and (regexp_like(sd1.tracking_detail_reason,'tu choi|thay doi dia chi phat|khong tim thay|sai') = false
    and regexp_like(sd2.tracking_detail_reason,'tu choi|thay doi dia chi phat|khong tim thay|sai') = false)
    

)
)
