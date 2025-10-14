with  sls_tracking AS (
   SELECT
    slo_id log_id
   , min(case when tracking_code in ('F100','F510') then from_unixtime((CASE WHEN (ctime > actual_time) THEN (actual_time - 3600) ELSE (ctime - 3600) END)) end ) pku_done_time
   , min(case when tracking_code in ('F980') then from_unixtime((CASE WHEN (ctime > actual_time) THEN (actual_time - 3600) ELSE (ctime - 3600) END)) end ) deli_done_time
   , min(case when tracking_code in ('F600','F650','F980','F668','F680','F999') then from_unixtime((CASE WHEN (ctime > actual_time) THEN (actual_time - 3600) ELSE (ctime - 3600) END)) end ) deli_1st_time
   FROM
    sls_mart.shopee_ssc_lts_tracking_vn_db__logistic_tracking_tab__reg_continuous_s0_live 
   GROUP BY 1
)
-- ,routes AS (
--    SELECT DISTINCT
--     start_state
--     , start_city
--     , end_state
--     , end_city
--     , route
--    FROM
--      vnbi_ops.routes_definition
--    WHERE (ingestion_timestamp = (SELECT max(ingestion_timestamp) col_1
-- FROM
--   vnbi_ops.routes_definition
-- ))
-- )
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
      ,o.buyer_shipping_address_state buyer_shipping_address_state
      ,o.buyer_shipping_address_city
      ,st.pku_done_time
      ,st.deli_done_time
      ,st.deli_1st_time
      , case when t1.fulfillment_channel_id = 50015 then cast((COALESCE(CAST(coalesce(slav.sla_vnp_ex,'0') AS double), 0) + COALESCE(CAST(coalesce(sl.extend_sla___addon,'0') AS double), 0)) as double)
        when t1.fulfillment_channel_id = 50016 then cast((COALESCE(CAST(coalesce(slav.sla_vnp_eco,'0') AS double), 0) + COALESCE(CAST(coalesce(sl.extend_sla___addon,'0') AS double), 0)) as double)
        else 0 end sla
      --, cast(date_diff('hour',st.pku_done_time,st.deli_1st_time)/24 as double) deli_1st_leadtime
      ,round((to_unixtime(st.deli_1st_time) -  to_unixtime(st.pku_done_time))/86400,1)  deli_1st_leadtime
      , r.route route
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
left join sls_mart.dwd_parcel_detail_union_nonsensitive_df_vn t1 on t1.grass_date = current_date - interval '1' day and t1.log_id = t3.log_id
left join SLS_tracking st on t3.log_id = st.log_id
left join vnbi_ops.vnbi_ops_log_sla_vnp_vtp slav on lower(o.seller_shipping_address_state) = lower(slav.start_state) and lower(o.buyer_shipping_address_state) = lower(slav.end_state)
                                                 and lower(o.seller_shipping_address_city) = lower(slav.start_city) and lower(o.buyer_shipping_address_city) = lower(slav.end_city)
                                                 and slav.ingestion_timestamp = (select max(ingestion_timestamp) col2 from vnbi_ops.vnbi_ops_log_sla_vnp_vtp)

LEFT JOIN vnbi_ops.vnbi_ops_log_sla_vnp_extend sl on lower(o.buyer_shipping_address_state) = lower(sl.state)
                                                 and lower(o.buyer_shipping_address_city) = lower(sl.city)
                                                 and lower(o.buyer_shipping_address_district) = lower(sl.district)
                                                 and sl.ingestion_timestamp = (select max(ingestion_timestamp) col3 from vnbi_ops.vnbi_ops_log_sla_vnp_extend)
-- left join routes r 
-- on lower(r.start_state) = lower(seller_shipping_address_state)
--         and lower(r.start_city) = lower(seller_shipping_address_city)
--         and lower(r.end_state) = lower(buyer_shipping_address_state)
--         and lower(r.end_city) = lower(buyer_shipping_address_city)
left join vnbi_ops.vnpeco_region sr on lower(sr.state) = lower(o.seller_shipping_address_state)
left join vnbi_ops.vnpeco_region br on lower(br.state) = lower(o.buyer_shipping_address_state)
left join vnbi_ops.vnpeco_route r on lower(r.ship_from) = lower(sr.region) and lower(r.ship_to) = lower(br.region)
left join holiday d on lower(t1.log_name) = lower(d.shipping_carrier) and (cast(d.holiday_start as date) between date(st.pku_done_time) and date(st.deli_1st_time))

where
      t1.fulfillment_channel_id in (50015,50016)

)
,data1 as (
    select *
    , from_unixtime(to_unixtime(pku_done_time) + cast((sla + d_sundays + d_holidays) as bigint)*3600*24) expected_delivery
    from data
    where date(from_unixtime(to_unixtime(pku_done_time) + cast((sla + d_sundays + d_holidays) as bigint)*3600*24)) between date '2022-11-14' and date '2022-11-20'

)
,data2 as (
    select *
    , from_unixtime(to_unixtime(pku_done_time) + cast((sla + d_sundays + d_holidays) as bigint)*3600*24) expected_delivery
    from data
    where date(from_unixtime(to_unixtime(pku_done_time) + cast((sla + d_sundays + d_holidays) as bigint)*3600*24)) between date '2022-11-21' and date '2022-11-27'

)
select 
                --month(expected_delivery) month_
        week(d1.expected_delivery)+1 week_
        --,date(expected_delivery) date_
                ,d1.shipping_carrier
                ,d1.buyer_shipping_address_state 
        --, zone
                --,seller_shipping_address_state
        --,buyer_shipping_address_state
                ,round(avg(d1.deli_1st_leadtime),1) as "wk47"
                ,round(avg(d2.deli_1st_leadtime),1) as "wk48"
        ,round(approx_percentile(d1.deli_1st_leadtime,0.8),1) as "percentile_80 wk47"
        ,round(approx_percentile(d2.deli_1st_leadtime,0.8),1) as "percentile_80 wk48"
        ,round(approx_percentile(d1.deli_1st_leadtime,0.95),1) as "percentile_95 wk47"
        ,round(approx_percentile(d1.deli_1st_leadtime,0.95),1) as "percentile_95 wk48"
        --,count(distinct lm_tracking_no) order_
        --,sum(case when deli_1st_leadtime <= sla then 1 else 0 end) gross_ontime
        --,(sum(case when deli_1st_leadtime <= sla then 1 else 0 end)/count(distinct lm_tracking_no)) ontime_ratio

from data1 d1
left join data2 d2 on d1.shipping_carrier = d2.shipping_carrier

group by 1,2,3
