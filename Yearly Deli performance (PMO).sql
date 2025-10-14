with multi as
(select cast(order_id as bigint) order_id, count(distinct forder_id) nb_parcels
    from oms_mart.shopee_oms_vn_db__fulfillment_order_tab__vn_daily_s0_live
    where whs_id is not null
    group by 1
    having count(distinct forder_id) > 1 
)
,routes AS (
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
, holiday AS
(   SELECT
    shipping_carrier,
    holiday_start,
    nb_holiday,
    ingestion_timestamp
    FROM vnbi_ops.shopee_vn_op_team__db_tpl_holiday as d
    WHERE d.ingestion_timestamp = (select max(ingestion_timestamp) max_timestamp4 from vnbi_ops.shopee_vn_op_team__db_tpl_holiday) AND type= 'delivery'
) 
, audit as 
(
    SELECT
    orderid
    ,new_status status
    ,min(from_unixtime(ctime)) min_ctime
    from (select * from marketplace.shopee_logistics_audit_v3_db__logistics_audit_tab__vn_continuous_s0_live where grass_region = 'VN')
    WHERE date(from_unixtime((ctime - 3600))) >= date'2021-11-01'--date'2021-03-01'
    GROUP by 1,2 
)
,sls_tracking as
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
,
pku as (
SELECT distinct lm_tracking_no
        ,t2.order_id
        , t2.oms_forder_id
        , if(inbound_type = 2, 1,0) is_drop_off 
        , whs_id as whs_code
        , lm_shipment_provider
        , case when whs_id = 'VNN' then 'Hà Nội'
           when whs_id in ('VNS','VNW') then 'TP. Hồ Chí Minh'
           else seller_address_state
      end as seller_address_state
        , ffm.log_id
        , from_unixtime(min(t2.order_arrange_shipping_timestamp - 3600)) as schedule_time 
        , from_unixtime(min(ffm.scheduled_pickup_timestamp)) as scheduled_pickup_time 
        , case when ffm.whs_id in ('VNN','VNS','VNW') then from_unixtime(min(ffm.lm_inbound_timestamp - 3600))
                else coalesce(sls.pku_done_tracking_time,pd.min_ctime,from_unixtime(min(ffm.lm_inbound_timestamp - 3600))) end as pickup_done_time
        , coalesce(dli_done_tracking_time,from_unixtime(min(ffm.lm_delivered_timestamp - 3600)),d.min_ctime) as delivery_done_time
        , coalesce(coalesce(dli_delivering_tracking_time,dli_pending_tracking_time,dli_done_tracking_time,return_ini_tracking_time,return_pending_tracking_time,return_done_tracking_time)
                    ,from_unixtime(min(coalesce(lm_first_attempt_delivery_timestamp,lm_delivered_timestamp,lm_first_failed_delivery_attempt_timestamp,lm_delivery_failed_timestamp,return_initiated_timestamp) - 3600)),d.min_ctime,df.min_ctime) as first_attempt_delivery_time
        , coalesce(sls.return_ini_tracking_time, sls.return_pending_tracking_time, sls.return_done_tracking_time,from_unixtime(min(coalesce(return_initiated_timestamp,return_pending_timestamp,return_completed_timestamp) - 3600))) AS failed_delivery_time
        ,sls.dli_pending_tracking_time dli_pending_tracking_time
        FROM sls_tracking sls
        left join 
        sls_mart.dwd_parcel_detail_union_nonsensitive_df_vn ffm on ffm.log_id = sls.log_id
        left join sls_mart.dwd_ofg_forder_union_df_vn t2 on t2.grass_date = current_date - interval '1' day and ffm.log_id = t2.log_id 
        left join sls_mart.dwd_order_detail_info_nonsensitive_df_vn t3 on t3.grass_date = current_date - interval '1' day and t3.order_id = t2.order_id
        left join audit s on  t2.order_id = s.orderid and s.status = 1 --scheduled
        left join audit pf on  t2.order_id = pf.orderid and pf.status in (3,4) --pickup retry, failed
        left join audit pd on  t2.order_id = pd.orderid and pd.status = 2 -- pickup done
        left join audit d on  t2.order_id = d.orderid and d.status = 5 --delivery done
        left join audit df on t2.order_id = df.orderid and df.status = 6 -- delivery failed
        left join audit l on  t2.order_id = l.orderid and l.status = 11 --lost
    
        WHERE 1=1
        --AND DATE(from_unixtime(ffm.order_create_timestamp - 3600)) > current_date - interval '160' day 
        AND ffm.grass_date = current_date - interval '1' day
        --and lost_timestamp is null
        --AND ffm.order_id = 85308200196423
        GROUP BY 1 ,2 ,3,4 ,5 ,6,7,8,pd.min_ctime,d.min_ctime,df.min_ctime,sls.pku_done_tracking_time,dli_delivering_tracking_time,dli_pending_tracking_time,dli_done_tracking_time,return_ini_tracking_time,return_pending_tracking_time,return_done_tracking_time
    )
    
    , base AS (
    SELECT distinct pku.lm_tracking_no
    , o.order_id
    , date_trunc('month', from_unixtime(create_timestamp - 3600)) as _month
    , buyer_shipping_address_state Buyer_State
    , buyer_shipping_address_city Buyer_City
    , buyer_shipping_address_district Buyer_District
    , CASE WHEN (is_cb_shop = 1 and sls.lm_shipment_provider IN ('VN_JNT','VN_J&T_N_Standard','VN_CB_SPX_N','CB_VN_SPX_N')) THEN 'Hà Nội' 
            WHEN (is_cb_shop = 1 and sls.lm_shipment_provider IN ('VN_JNT_S','VN_J&T_S_Standard','VN_CB_SPX_S','CB_VN_SPX_S')) THEN 'TP. Hồ Chí Minh' 
            when multi.order_id is not null then w.state
            when sls.whs_id = 'VNN' then 'Hà Nội'
            when sls.whs_id  in ('VNS','VNW') then 'TP. Hồ Chí Minh'
            ELSE seller_shipping_address_state 
            END seller_state
    , CASE WHEN (is_cb_shop = 1 and sls.lm_shipment_provider IN ('VN_JNT','VN_J&T_N_Standard','VN_CB_SPX_N','CB_VN_SPX_N')) THEN 'Quận Long Biên' 
            WHEN (is_cb_shop = 1 and sls.lm_shipment_provider IN ('VN_JNT_S','VN_J&T_S_Standard','VN_CB_SPX_S','CB_VN_SPX_S')) THEN 'Huyện Củ Chi' 
            when multi.order_id is not null then w.city
            when sls.whs_id = 'VNN' then 'Quận Long Biên'
            when sls.whs_id  in ('VNS','VNW') then 'Huyện Củ Chi'
            ELSE seller_shipping_address_city 
            END Seller_City
    , seller_shipping_address_district seller_district
    , is_drop_off
    , sl.route route
    , fulfilment_shipping_carrier
    , (CASE WHEN (is_cb_shop = 1 AND (sls.lm_shipment_provider IN ('VN_JNT', 'VN_J&T_N_Standard', 'VN_JNT_S', 'VN_J&T_S_Standard'))) THEN 'J&T Express' WHEN (is_cb_shop = 1 AND (sls.lm_shipment_provider IN ('VN_CB_SPX_N', 'CB_VN_SPX_N', 'VN_CB_SPX_S', 'CB_VN_SPX_S', 'VNR_ID_SPX_SIP_N', 'VNR_ID_SPX_SIP_S'))) THEN 'Shopee Express' ELSE o.fulfilment_shipping_carrier END) "tpl_name"
    , (CASE WHEN (is_cb_shop = 1 AND (sls.lm_shipment_provider IN ('VN_JNT', 'VN_J&T_N_Standard', 'VN_JNT_S', 'VN_J&T_S_Standard'))) THEN 'J&T - CB' WHEN (is_cb_shop = 1 AND (sls.lm_shipment_provider IN ('VN_CB_SPX_N', 'CB_VN_SPX_N', 'VN_CB_SPX_S', 'CB_VN_SPX_S', 'VNR_ID_SPX_SIP_N', 'VNR_ID_SPX_SIP_S'))) THEN 'SPX - CB' when o.fulfilment_channel_id = 50021 then 'Shopee Xpress' ELSE o.fulfilment_shipping_carrier END) "tpl_call"
    , pku.pickup_done_time
    , pku.first_attempt_delivery_time 
    , pku.delivery_done_time dli_done
    , pku.failed_delivery_time delivery_failed
    ,pku.delivery_done_time delivery_done_time
    ,pku.dli_pending_tracking_time dli_pending_tracking_time
    , fulfilment_channel_id
    , sls.fulfillment_channel_id client_channel_id

    FROM pku 
    LEFT JOIN mp_order.dwd_order_all_ent_df__vn_s0_live o on o.grass_date> date '2021-01-01' and pku.order_id = o.order_id
    left join routes as sl
    on lower(sl.start_state) = lower(seller_shipping_address_state)
        and lower(sl.start_city) = lower(seller_shipping_address_city)
        and lower(sl.end_state) = lower(buyer_shipping_address_state)
        and lower(sl.end_city) = lower(buyer_shipping_address_city)
    -- left join routes as br
    -- on lower(br.address_state) =  lower(o.buyer_shipping_address_state)
    left join sls_mart.dwd_parcel_detail_union_nonsensitive_df_vn sls on sls.grass_date = current_date - interval '1' day
    and sls.log_id = pku.log_id
    left join multi on multi.order_id = pku.order_id
    left join oms_mart.shopee_oms_vn_db__fulfillment_order_tab__vn_daily_s0_live oms
    on oms.forder_id = pku.oms_forder_id
    left join oms_mart.shopee_oms_vn_db__warehouse_tab__reg_continuous_s0_live w
    on oms.whs_id = w.whs_id

    WHERE true 
    and date(pku.first_attempt_delivery_time) between current_date - interval '6' month and current_date
    --and is_bi_excluded = 0
    and first_attempt_delivery_time is not null 
    --and logistics_status <> 'LOST'
    and is_cb_shop = 0
    and sls.fulfillment_channel_id not in (50025,50028,50029)
    -- and fulfilment_channel_id = 50024
    
)
, base1 as 
( select b.*
 , cast (coalesce((case when cast(d.nb_holiday as int) is null then 0 else cast(d.nb_holiday as int) end),0) as double) as d_holidays
 --, cast(sla.original_sla as double) original_sla
 , cast(coalesce((CASE  WHEN (date_diff('day', pickup_done_time, first_attempt_delivery_time) < 7 and day_of_week(first_attempt_delivery_time) < day_of_week(pickup_done_time)) then 1 
            WHEN (date_diff('day', pickup_done_time, first_attempt_delivery_time) < 7 and day_of_week(first_attempt_delivery_time) >= day_of_week(pickup_done_time)) and date_format(first_attempt_delivery_time, '%W') IN ('Sunday') then 1
            WHEN (date_diff('day', pickup_done_time, first_attempt_delivery_time) >= 7 and date_format(first_attempt_delivery_time, '%W') IN ('Sunday') and date_format(pickup_done_time, '%W') IN ('Sunday')) then date_diff('week', pickup_done_time, first_attempt_delivery_time)
            else (date_diff('week', pickup_done_time, first_attempt_delivery_time)
                - CASE WHEN date_format(first_attempt_delivery_time, '%W') IN ('Sunday') THEN 1 ELSE 0 END
                - CASE WHEN date_format(pickup_done_time, '%W') IN ('Sunday') THEN 1 ELSE 0 END) END),0) as double) As d_sundays
 , case when b.client_channel_id = 50015 then cast((COALESCE(CAST(coalesce(slav.sla_vnp_ex,'0') AS double), 0) + COALESCE(CAST(coalesce(sl.extend_sla___addon,'0') AS double), 0)) as double)
        when b.client_channel_id = 50016 then cast((COALESCE(CAST(coalesce(slav.sla_vnp_eco,'0') AS double), 0) + COALESCE(CAST(coalesce(sl.extend_sla___addon,'0') AS double), 0)) as double)
        when b.client_channel_id = 50021 then cast(COALESCE(CAST(coalesce(sla.spx,'0') AS double), 0) as double)
        when b.client_channel_id = 50011 then cast(COALESCE(CAST(coalesce(sla.ghn,'0') AS double), 0) as double)
        when b.fulfilment_channel_id = 50023 then cast(COALESCE(CAST(coalesce(sla.njv,'0') AS double), 0) as double)
        when b.client_channel_id = 50024 then cast(COALESCE(CAST(coalesce(sla.best,'0') AS double), 0) as double)
        when b.fulfilment_channel_id = 50018 then cast(COALESCE(CAST(coalesce(sla.jnt,'0') AS double), 0) as double)
        when b.client_channel_id = 50010 and rd.delivery_area = 'Urban' then cast(COALESCE(CAST(coalesce(slav.sla_vtp_urban,'0') AS double), 0) as double)
        when b.client_channel_id = 50010 and rd.delivery_area = 'Rural' then cast(COALESCE(CAST(coalesce(slav.sla_vtp_rural,'0') AS double), 0) as double)
        when b.client_channel_id = 50010 and rd.delivery_area = 'Hard to delivery zone' then (cast(COALESCE(CAST(coalesce(slav.sla_vtp_urban,'0') AS double), 0) as double) + cast(COALESCE(CAST(coalesce(rd.extend_sla___addon,'0') AS double), 0) as double))
        when b.client_channel_id = 50012 and ((extract( hour from pickup_done_time ) < 12 ) or (extract( hour from pickup_done_time ) = 12 
        and extract( minute  from pickup_done_time ) = 0 and extract( second  from pickup_done_time ) = 0 )) then cast(COALESCE(CAST(coalesce(sla.ghtk_1,'0') AS double), 0) as double)
        when b.client_channel_id = 50012 and ((extract( hour from pickup_done_time ) >= 12 and extract( hour from pickup_done_time ) < 18) or (extract( hour from pickup_done_time ) = 18 
        and extract( minute  from pickup_done_time ) = 0 and extract( second  from pickup_done_time ) = 0 )) then cast(COALESCE(CAST(coalesce(sla.ghtk_2,'0') AS double), 0) as double)
        when b.client_channel_id = 50012 and extract( hour from pickup_done_time ) >= 18 then cast(COALESCE(CAST(coalesce(sla.ghtk_3,'0') AS double), 0) as double)
        end total_sla

from base b 
left join holiday d on lower(b."tpl_call") = lower(d.shipping_carrier) and (cast(d.holiday_start as date) between date(b.pickup_done_time) and date(b.first_attempt_delivery_time))
left join vnbi_ops.vnbi_ops_log_sla_part_1 sla on lower(b."Seller_State") = lower(sla.start_state) and lower(b."Buyer_State") = lower(sla.end_state)
                                               and lower(b.Seller_City) = lower(sla.start_city) and lower(b.Buyer_City) = lower(sla.end_city)
                                               and sla.ingestion_timestamp = (select max(ingestion_timestamp) col1 from vnbi_ops.vnbi_ops_log_sla_part_1)

left join vnbi_ops.vnbi_ops_log_sla_vnp_vtp slav on lower(b."Seller_State") = lower(slav.start_state) and lower(b."Buyer_State") = lower(slav.end_state)
                                                 and lower(b.Seller_City) = lower(slav.start_city) and lower(b.Buyer_City) = lower(slav.end_city)
                                                 and slav.ingestion_timestamp = (select max(ingestion_timestamp) col2 from vnbi_ops.vnbi_ops_log_sla_vnp_vtp)

LEFT JOIN vnbi_ops.vnbi_ops_log_sla_vnp_extend sl on lower(b."Buyer_State") = lower(sl.state)
                                                 and lower(b.Buyer_City) = lower(sl.city)
                                                 and lower(b.Buyer_District) = lower(sl.district)
                                                 and sl.ingestion_timestamp = (select max(ingestion_timestamp) col3 from vnbi_ops.vnbi_ops_log_sla_vnp_extend)
LEFT JOIN vnbi_ops.vnbi_ops_log_sla_vtp_extend rd on lower(b."Buyer_State") = lower(rd.state)
                                                 and lower(b.Buyer_City) = lower(rd.city)
                                                 and lower(b.Buyer_District) = lower(rd.district)
                                                 and rd.ingestion_timestamp = (select max(ingestion_timestamp) col4 from vnbi_ops.vnbi_ops_log_sla_vtp_extend)

)
, base2 AS
( select distinct lm_tracking_no
        , pickup_done_time
        , Buyer_State
        , Buyer_City
        , Seller_State
        , "tpl_call"
        , first_attempt_delivery_time
,delivery_done_time
,dli_pending_tracking_time
,delivery_failed
        , route
        , fulfilment_channel_id
        , total_sla
        -- , from_unixtime(to_unixtime(pickup_done_time) + cast((case when total_sla is null then (case when buyer_zone = 'Urban' then (3.2 + d_sundays + d_holidays)
        --                                         when buyer_zone = 'Rural' then (4.2 + d_sundays + d_holidays)
        --                                         else (3.7 + d_sundays + d_holidays) end) 
        --       else cast((total_sla + d_sundays + d_holidays) as double)
        --       end) as bigint)*3600*24) expected_delivery
        , case when client_channel_id = 50024 then cast(date_add('hour',24,cast(date(from_unixtime(to_unixtime(pickup_done_time) + cast((total_sla + d_sundays + d_holidays) as bigint)*3600*24)) as timestamp)) as timestamp)
         else from_unixtime(to_unixtime(pickup_done_time) + cast((total_sla + d_sundays + d_holidays) as bigint)*3600*24) end expected_delivery 

 from base1 b
 )
 
, base3 AS (
select distinct lm_tracking_no
        , pickup_done_time
        , Buyer_State
        , Buyer_City
        , "tpl_call"
        , first_attempt_delivery_time
        , delivery_done_time
        , delivery_failed
        ,dli_pending_tracking_time
        ,case 
            when date(first_attempt_delivery_time) > date(expected_delivery) then 'Late'
            when date(first_attempt_delivery_time) <= date(expected_delivery) then 'Ontime'
            else NULL
        end as Check_delivery_ontime
        , route
        ,expected_delivery
        from base2 as b 
        where date(expected_delivery) between date '2022-06-01' and date '2022-11-30'
        and "tpl_call" in ('VNPost Nhanh','VNPost Tiết Kiệm')
)

    Select month(expected_delivery) month
        , Buyer_State
        , Buyer_City
        , "tpl_call"
        , sum(case when pickup_done_time is not null then 1 else 0 end) deli_request
        , sum(case when delivery_done_time is not null then 1 else 0 end) deli_done
        , sum(case when Check_delivery_ontime = 'Ontime' then 1 else 0 end) ontime_delivery
        , sum(case when Check_delivery_ontime = 'Late' then 1 else 0 end) late_delivery
        , sum(case when dli_pending_tracking_time is null and delivery_done_time is not null  then 1 else 0 end) deli_done_1st_time
        , sum(case when delivery_failed is not null then 1 else 0 end) deli_failed
--        , count(distinct lm_tracking_no) total_order
        , route
        
From base3
Group by 1,2,3,4,11--,5--,6,7--,8--,9
