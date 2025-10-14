with  sls_tracking AS (
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
, data as
(
select distinct
      t1.lm_tracking_no lm_tracking_no
      ,case when t1.fulfillment_channel_id = 50015 then 'VNPost Nhanh'
      when t1.fulfillment_channel_id = 50016 then 'VNPost Tiết Kiệm'
      else null end shipping_carrier
      ,o.seller_shipping_address_state 
      ,o.seller_shipping_address_city 
      ,o.buyer_shipping_address_state 
      ,o.buyer_shipping_address_city 
      ,o.buyer_shipping_address_district 
      ,st.pku_done_time 
      ,st.deli_done_time
      ,st.deli_1st_time 
      ,st.deli_failed_time 
      ,st.dli_pending_tracking_time,
      case when t1.fulfillment_channel_id = 50015 then cast((COALESCE(CAST(coalesce(slav.sla_vnp_ex,'0') AS double), 0) + COALESCE(CAST(coalesce(sl.extend_sla___addon,'0') AS double), 0)) as double)
        when t1.fulfillment_channel_id = 50016 then cast((COALESCE(CAST(coalesce(slav.sla_vnp_eco,'0') AS double), 0) + COALESCE(CAST(coalesce(sl.extend_sla___addon,'0') AS double), 0)) as double)
        else 0 end sla
      , cast(date_diff('hour',st.pku_done_time,st.deli_done_time)/24 as double) deli_done_leadtime
      , cast(date_diff('hour',st.pku_done_time,st.deli_1st_time)/24 as double) deli_1st_leadtime
    --       , r.route
      --, sl.delivery_area zone
      , cast(coalesce((CASE  WHEN (date_diff('day', st.pku_done_time, st.deli_1st_time) < 7 and day_of_week(st.deli_1st_time) < day_of_week(st.pku_done_time)) then 1 
            WHEN (date_diff('day', st.pku_done_time, st.deli_1st_time) < 7 and day_of_week(st.deli_1st_time) >= day_of_week(st.pku_done_time)) and date_format(st.deli_1st_time, '%W') IN ('Sunday') then 1
            WHEN (date_diff('day', st.pku_done_time, st.deli_1st_time) >= 7 and date_format(st.deli_1st_time, '%W') IN ('Sunday') and date_format(st.pku_done_time, '%W') IN ('Sunday')) then date_diff('week', st.pku_done_time, st.deli_1st_time)
            else (date_diff('week', st.pku_done_time, st.deli_1st_time)
                - CASE WHEN date_format(st.deli_1st_time, '%W') IN ('Sunday') THEN 1 ELSE 0 END
                - CASE WHEN date_format(st.pku_done_time, '%W') IN ('Sunday') THEN 1 ELSE 0 END) END),0) as double) As d_sundays
        , cast (coalesce((case when cast(d.nb_holiday as int) is null then 0 else cast(d.nb_holiday as int) end),0) as double) as d_holidays
      
from (select * from mp_order.dwd_order_all_ent_df__vn_s0_live where grass_date > date '2022-01-01' ) as o
left join sls_mart.dwd_ofg_forder_union_df_vn t3 on t3.grass_date = current_date - interval '1' day and t3.order_id = o.order_id
left join sls_mart.dwd_parcel_detail_union_nonsensitive_df_vn t1 on t1.grass_date = current_date - interval '1' day and t1.log_id = t3.log_id
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
left join vnbi_ops.vnpeco_route r on lower(r.ship_from) = lower(sr.region) and lower(r.ship_to) = lower(br.region)
left join holiday d on lower(t1.log_name) = lower(d.shipping_carrier) and (cast(d.holiday_start as date) between date(st.pku_done_time) and date(st.deli_1st_time))

where t1.fulfillment_channel_id in (50015,50016)


)
,data1 as (
    select *
    , from_unixtime(to_unixtime(pku_done_time) + cast((sla + d_sundays + d_holidays) as bigint)*3600*24) expected_delivery
    from data
)
select
    --week(pku_done_time)+1 week_ 
    o.shipping_carrier
    ,o.lm_tracking_no
    ,o.buyer_shipping_address_state
    ,o.buyer_shipping_address_city
    ,o.buyer_shipping_address_district
    , o.expected_delivery "Ngày dự kiến giao"
    , o.deli_1st_time "Liên hệ phát lần đầu"
    , o.deli_done_time "Giao hàng thành công"
    , o.deli_failed_time "Phát hoàn thành công"
    ,case when deli_1st_leadtime <= sla then 'Liên hệ lần đầu đúng hẹn' else 'Liên hệ lần đầu trễ' end "Liên hệ lần đầu"
    ,case when deli_done_time is not null and dli_pending_tracking_time is null then 'Giao thành công lần đầu' else 'Giao không thành công lần đầu' end  as "Kiểm tra giao lần đầu thành công"
    ,case when deli_failed_time is not null then 'Hoàn thành công' else 'Đơn không hoàn' end as "Kiểm tra đơn hoàn"
    ,case when deli_done_time is not null then 'Giao thành công' else 'Không giao thành công' end "Kiểm tra đơn giao thành công"
    ,(avg(round(((to_unixtime(o.deli_1st_time) - to_unixtime(o.pku_done_time))/86400),1))) gap_time



from data1 o
where date(o.deli_done_time) between date '2022-11-02' and date '2022-11-02'
and o.buyer_shipping_address_state in ('TP. Hồ Chí Minh')
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
