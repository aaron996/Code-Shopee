with st_dli_1 as
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
, sls_tracking AS (
   SELECT
    slo_id log_id
   , min(case when tracking_code in ('F100','F510') then from_unixtime((CASE WHEN (ctime > actual_time) THEN (actual_time - 3600) ELSE (ctime - 3600) END)) end ) pku_done_time
   , min(case when tracking_code = 'F980' then from_unixtime((CASE WHEN (ctime > actual_time) THEN (actual_time - 3600) ELSE (ctime - 3600) END)) end ) deli_done_time
    , min(case when tracking_code = 'F999' then from_unixtime((CASE WHEN (ctime > actual_time) THEN (actual_time - 3600) ELSE (ctime - 3600) END)) end ) deli_failed_time
   , min(case when tracking_code in ('F600','F650','F980','F668','F680','F999') then from_unixtime((CASE WHEN (ctime > actual_time) THEN (actual_time - 3600) ELSE (ctime - 3600) END)) end ) deli_1st_time,
    min(case when tracking_code = 'F650' then from_unixtime(actual_time-3600) else null end) as dli_pending_tracking_time
FROM
    sls_mart.shopee_ssc_lts_tracking_vn_db__logistic_tracking_tab__reg_continuous_s0_live 
   GROUP BY 1
)
, holiday AS
(   SELECT
    shipping_carrier,
    holiday_start,
    nb_holiday,
    ingestion_timestamp
    FROM vnbi_ops.shopee_vn_op_team__db_tpl_holiday as d
    WHERE d.ingestion_timestamp = (select max(ingestion_timestamp) max_timestamp4 from vnbi_ops.shopee_vn_op_team__db_tpl_holiday) AND type= 'delivery'
)
, routes AS (
   SELECT DISTINCT
    start_state
    , start_city
    , end_state
    , end_city
    , route
   FROM
     vnbi_ops.routes_definition
   WHERE (ingestion_timestamp = (SELECT "max"(ingestion_timestamp) col_1
FROM
  vnbi_ops.routes_definition
))
)
, data as
(
select distinct
      t1.lm_tracking_no lm_tracking_no
      ,o.fulfilment_shipping_carrier
      ,o.fulfilment_channel_id
      ,o.seller_shipping_address_state 
      ,o.seller_shipping_address_city 
      ,o.buyer_shipping_address_state
      ,o.buyer_shipping_address_city
      ,o.buyer_shipping_address_district
      ,o.logistics_status_id
      ,st.pku_done_time
      ,st.deli_done_time deli_done_time
      ,st.deli_1st_time deli_1st_time
      ,st.deli_failed_time deli_failed_time
      ,st.dli_pending_tracking_time
      ,o.shop_id
,case when o.is_preferred_shop = 1 then 'preferred_shop' 
    when o.is_preferred_plus_shop = 1 then 'preferred_plus_shop' 
    when o.is_official_shop = 1 then 'shop Mall' 
    else 'no' 
    end as seller_type
--,o.shop_name

--,case when t3.inbound_type = 1 then 'pickup' when t3.inbound_type = 2 then 'dropoff' else null end
--,case when multi.nb_parcels IS NOT NULL then 'Multi WH' else 'Not Multi WH' end as "Check_Multi_WH"
--,o.escrow_to_seller_amt escrow_to_seller
,case when o.gmv <= 100000 then '<100k'
      when o.gmv <= 200000 then '100-200k' 
      when o.gmv > 200000 then '>200k' 
      else 'no' end as grand_total
,case when t3.parcel_chargeable_weight <= 1000 then '<1kg'
      when t3.parcel_chargeable_weight <= 2000 then '1-2kg'
      when t3.parcel_chargeable_weight <= 3000 then '2-3kg'
      when t3.parcel_chargeable_weight <= 4000 then '3-4kg'
      when t3.parcel_chargeable_weight <= 5000 then '4-5kg'
      when t3.parcel_chargeable_weight > 5000 then '>5kg'
      else 'no' end as actual_weight
,case 
        when (hour(st.dli_pending_tracking_time)) > 21 then 'Sau 21h' 
        when (hour(st.dli_pending_tracking_time)) < 6 then 'Trước 6h' 
        else 'Bình thường' end as "Thời gian giao hàng"
,case when (sd1.tracking_time is not null and sd2.tracking_time is not null and (cast(to_unixtime(sd2.tracking_time) as double) - cast(to_unixtime(sd1.tracking_time)as double)) < 900 
        or (sd2.tracking_time is not null and sd3.tracking_time is not null and (cast(to_unixtime(sd3.tracking_time) as double) - cast(to_unixtime(sd2.tracking_time)as double)) < 900)) then 'Gọi 2 lần trong vòng 15 phút'
      when ((sd2.tracking_time is not null and hour(sd1.tracking_time) < 13 and hour(sd2.tracking_time) > 12)
       or (sd3.tracking_time is not null and hour(sd2.tracking_time) < 13 and hour(sd3.tracking_time) > 12)) then '2 ca sáng chiều'
      when ((sd2.tracking_time is not null and to_unixtime(sd2.tracking_time) - to_unixtime(sd1.tracking_time) < 18000)
       or (sd3.tracking_time is not null and to_unixtime(sd3.tracking_time) - to_unixtime(sd2.tracking_time) < 18000)) then 'Không hợp lệ'
      else 'Hợp lệ'
      end as "Thời gian giữa 2 ca giao"  
,(case when o.payment_method = 'PAY_COD' then 'COD' else 'CC' end) as payment_method
,case 
        when t1.fulfillment_channel_id = 50015 then cast((COALESCE(CAST(coalesce(slav.sla_vnp_ex,'0') AS double), 0) + COALESCE(CAST(coalesce(sl.extend_sla___addon,'0') AS double), 0)) as double)
        when t1.fulfillment_channel_id = 50016 then cast((COALESCE(CAST(coalesce(slav.sla_vnp_eco,'0') AS double), 0) + COALESCE(CAST(coalesce(sl.extend_sla___addon,'0') AS double), 0)) as double)
        else 0 end sla
      , cast(date_diff('hour',st.pku_done_time,st.deli_done_time)/24 as double) deli_done_leadtime
      , round((to_unixtime(st.deli_1st_time) -  to_unixtime(st.pku_done_time))/86400,1) deli_1st_leadtime
        , sl.route route
        , sl.delivery_area zone
      , cast(coalesce((CASE  WHEN (date_diff('day', st.pku_done_time, st.deli_1st_time) < 7 and day_of_week(st.deli_1st_time) < day_of_week(st.pku_done_time)) then 1 
            WHEN (date_diff('day', st.pku_done_time, st.deli_1st_time) < 7 and day_of_week(st.deli_1st_time) >= day_of_week(st.pku_done_time)) and date_format(st.deli_1st_time, '%W') IN ('Sunday') then 1
            WHEN (date_diff('day', st.pku_done_time, st.deli_1st_time) >= 7 and date_format(st.deli_1st_time, '%W') IN ('Sunday') and date_format(st.pku_done_time, '%W') IN ('Sunday')) then date_diff('week', st.pku_done_time, st.deli_1st_time)
            else (date_diff('week', st.pku_done_time, st.deli_1st_time)
                - CASE WHEN date_format(st.deli_1st_time, '%W') IN ('Sunday') THEN 1 ELSE 0 END
                - CASE WHEN date_format(st.pku_done_time, '%W') IN ('Sunday') THEN 1 ELSE 0 END) END),0) as double) As d_sundays
        , cast (coalesce((case when cast(d.nb_holiday as int) is null then 0 else cast(d.nb_holiday as int) end),0) as double) as d_holidays
      
from (select * from mp_order.dwd_order_all_ent_df__vn_s0_live where grass_date > date '2022-01-01' ) as o
left join sls_mart.dwd_ofg_forder_union_df_vn t3 on t3.grass_date = current_date - interval '1' day and t3.order_id = o.order_id
left join sls_mart.dwd_parcel_detail_union_nonsensitive_v2_df_vn t1 on t1.grass_date = current_date - interval '1' day and t1.log_id = t3.log_id
left join SLS_tracking st on t3.log_id = st.log_id
left join vnbi_ops.vnbi_ops_log_sla_vnp_vtp slav on lower(o.seller_shipping_address_state) = lower(slav.start_state) and lower(o.buyer_shipping_address_state) = lower(slav.end_state)
                                                 and lower(o.seller_shipping_address_city) = lower(slav.start_city) and lower(o.buyer_shipping_address_city) = lower(slav.end_city)
                                                 and slav.ingestion_timestamp = (select max(ingestion_timestamp) col2 from vnbi_ops.vnbi_ops_log_sla_vnp_vtp)

LEFT JOIN vnbi_ops.vnbi_ops_log_sla_vnp_extend sl on lower(o.buyer_shipping_address_state) = lower(sl.state)
                                                 and lower(o.buyer_shipping_address_city) = lower(sl.city)
                                                 and lower(o.buyer_shipping_address_district) = lower(sl.district)
                                                 and sl.ingestion_timestamp = (select max(ingestion_timestamp) col3 from vnbi_ops.vnbi_ops_log_sla_vnp_extend)
left join vnbi_ops.vnpeco_region sr on lower(sr.state) = lower(o.seller_shipping_address_state)
left join vnbi_ops.vnpeco_region br on lower(br.state) = lower(o.buyer_shipping_address_state)
left join routes as sl
    on lower(sl.start_state) = lower(seller_shipping_address_state)
        and lower(sl.start_city) = lower(seller_shipping_address_city)
        and lower(sl.end_state) = lower(buyer_shipping_address_state)
        and lower(sl.end_city) = lower(buyer_shipping_address_city)
left join holiday d on lower(t1.lm_shipment_company) = lower(d.shipping_carrier) and (cast(d.holiday_start as date) between date(st.pku_done_time) and date(st.deli_1st_time))
LEFT JOIN st_dli_1 sd1 on sd1.slo_id = t1.log_id
LEFT JOIN st_dli_2 sd2 on sd2.slo_id = t1.log_id
LEFT JOIN st_dli_3 sd3 on sd3.slo_id = t1.log_id


)
,data1 as (
    select *
    , from_unixtime(to_unixtime(pku_done_time) + cast((sla + d_sundays + d_holidays) as bigint)*3600*24) expected_delivery
    from data
)
select
--concat(cast(month(deli_failed_time)as varchar ),'/',cast(year(deli_failed_time)as varchar )) month_    --,t1.lm_tracking_no
    
    lm_tracking_no
    ,fulfilment_shipping_carrier
    ,seller_shipping_address_state
    ,seller_shipping_address_city
    ,buyer_shipping_address_state
    ,buyer_shipping_address_city
    ,route
    ,actual_weight
    ,grand_total
    ,payment_method
    ,"Thời gian giao hàng"
    ,"Thời gian giữa 2 ca giao"
    --,approx_percentile(deli_1st_leadtime,0.95) as percentile_95
    --,sum(case when pku_done_time is not null then 1 else 0 end) gross_deli_request
    --,sum(case when date(deli_1st_time) <= date(expected_delivery) then 1 else 0 end) gross_ontime_deli_1st
    --,sum(case when deli_failed_time is not null then 1 else 0 end) gross_failed

    --,round(cast(sum(case when deli_done_time is not null then 1 else 0 end)as double)/cast(sum(case when pku_done_time is not null then 1 else 0 end) as double),3) "deli done rate"    
    --,round(cast(sum(case when deli_1st_leadtime <= sla then 1 else 0 end)as double)/cast(sum(case when (pku_done_time is not null and expected_delivery is not null) then 1 else 0 end)as double),3) "on time rate"
    --,round(cast(sum(case when deli_failed_time is not null then 1 else 0 end)as double)/cast(sum(case when pku_done_time is not null then 1 else 0 end)as double),3) "failed rate"
    --,round(cast(sum(case when deli_done_time is not null and dli_pending_tracking_time is null then 1 else 0 end )as double)/cast(sum(case when deli_done_time is not null then 1 else 0 end)as double),3) "1st delidone rate"
    --,round(avg(((to_unixtime(deli_done_time) - to_unixtime(deli_1st_time))/86400)),3) gap_time_deli_done
    --,round(avg(((to_unixtime(deli_1st_time) - to_unixtime(pku_done_time))/86400)),3) gap_time_1st



from data1 
where lm_tracking_no in ('CE217685146VN',
'CF215810084VN',
'CT214575212VN',
'EZ346308635VN',
'CL218552870VN',
'CM212812789VN',
'CF214597875VN',
'CF219226196VN',
'CT218513947VN',
'CL216763244VN',
'CT215553641VN',
'CM214605311VN',
'CF216698421VN',
'CT218504225VN',
'CL217677652VN',
'CE218790830VN',
'CF211269791VN',
'EZ343939938VN',
'CT213836415VN',
'CT218525945VN',
'CF213891862VN',
'CE215520833VN',
'CF219476432VN')
