WITH
 pku_done_tracking AS (
   SELECT
    slo_id log_id
   , min(case when tracking_code in ('F100','F510') then from_unixtime((CASE WHEN (ctime > actual_time) THEN (actual_time - 3600) ELSE (ctime - 3600) END)) end ) pku_done_time
   , min(case when tracking_code in ('F001','F002','F100','F510','F050','F097','F098') then from_unixtime((CASE WHEN (ctime > actual_time) THEN (actual_time - 3600) ELSE (ctime - 3600) END)) end ) pku_1st_time
    , min(case when tracking_code in ('F097') then from_unixtime((CASE WHEN (ctime > actual_time) THEN (actual_time - 3600) ELSE (ctime - 3600) END)) end ) pku_failed_time 
   FROM
    sls_mart.shopee_ssc_lts_tracking_vn_db__logistic_tracking_tab__reg_continuous_s0_live 
   GROUP BY 1
)
, Final AS (
   SELECT distinct t3.order_id
   , t1.lm_tracking_no
   , from_unixtime(coalesce(rd.pickup_time,t1.scheduled_pickup_timestamp)) scheduled_date
   , pku_done_time
   , pku_failed_time
   , t1.fulfillment_channel_id
   , CASE WHEN (year(from_unixtime(coalesce(rd.pickup_time,t1.scheduled_pickup_timestamp))) = 1970) THEN from_unixtime(t3.order_arrange_shipping_timestamp)
            ELSE from_unixtime(coalesce(rd.pickup_time,t1.scheduled_pickup_timestamp)) END as scheduled_date1
    ,t5.pku_1st_time
       , (CASE --when t1.inbound_type = 1 then 'pickup' 
            --when t1.inbound_type = 2 then 'drop-off'
                                    WHEN year(from_unixtime(rd.pickup_time-3600)) > 1970 THEN 'pickup'
                                    WHEN (year(from_unixtime(rd.pickup_time-3600)) = 1970 AND year(from_unixtime(t3.order_arrange_shipping_timestamp-3600)) > 1970) THEN 'drop-off'
                                    ELSE NULL
                                END) 
                                shipment_type
    , case when t1.inbound_type = 1 then 'pickup'
          when t1.inbound_type = 2 then 'dropoff'
          else null end  shipment_type1
    , (CASE WHEN ((year(from_unixtime(t1.scheduled_pickup_timestamp)) > 1970) AND (t1.scheduled_pickup_timestamp IS NOT NULL)) THEN 1 ELSE 0 END) gross_pickup_request
    , (CASE WHEN (date(pku_done_time) <= date(from_unixtime(t1.scheduled_pickup_timestamp))) THEN 1 ELSE 0 END) ontime_pickup_done
    , (CASE WHEN (date(pku_1st_time) <= date(from_unixtime(t1.scheduled_pickup_timestamp))) THEN 1 ELSE 0 END) ontime_pickup
    , (CASE WHEN ((year(from_unixtime(t1.scheduled_pickup_timestamp)) > 1970) AND (t1.scheduled_pickup_timestamp IS NOT NULL) and pku_done_time is not null) THEN 1 ELSE 0 END) gross_pickup_done
    , (CASE WHEN pku_failed_time is not null THEN 1 ELSE 0 END) gross_pickup_failed
    ,t3.order_arrange_shipping_timestamp Logistics_request_created
   , date_diff('hour',from_unixtime(t3.order_arrange_shipping_timestamp),pku_done_time) pickup_leadtime
    , case
    when t1.parcel_logistics_status_code = 1000 then 'NEW'
    when t1.parcel_logistics_status_code in (1001,1040) then 'INFO_RECEIVED'
    when t1.parcel_logistics_status_code in (1061,1051,2001,2008,2009,2015,2030,2040,3001,3060,3080,3090) then 'IN_TRANSIT'
    when t1.parcel_logistics_status_code in (9997,3099,3070) then 'EXCEPTION'
    when t1.parcel_logistics_status_code = 9999 then 'CANCELLED'
    when t1.parcel_logistics_status_code = 1050 then 'DOMESTIC_PICKUP'
    when t1.parcel_logistics_status_code in  (4010,4020,4100) then 'DOMESTIC_SORTING'
    when t1.parcel_logistics_status_code = 4060 then 'DOMESTIC_DELIVERING'
    when t1.parcel_logistics_status_code = 4097 then 'DOMESTIC_DELIVERED'
    when t1.parcel_logistics_status_code = 4070 then 'DELIVERY_PENDING'
    when t1.parcel_logistics_status_code = 9998 then 'PARCEL_LOST'
    when t1.parcel_logistics_status_code =  8020 then 'PARCEL_RETURN'
    when t1.parcel_logistics_status_code = 8050 then 'RETURN_COMPLETED'
    when t1.parcel_logistics_status_code = 8099 then 'RETURN_FAILED'
    when t1.parcel_logistics_status_code = 8045 then 'PENDING_FOR_RETURN'
    when t1.parcel_logistics_status_code in (4098,8001,4102) then 'REQUEST_RETURN'
    when t1.parcel_logistics_status_code in (996,1041,8010) then 'PICKUP_RETRY'
    when t1.parcel_logistics_status_code in (1099,2099,9995) then 'DOMESTIC_PICKUP_FAILED'
    when t1.parcel_logistics_status_code = 1060 then 'DOMESTIC_DROPOFF'
    when t1.parcel_logistics_status_code = 1042 then 'DOMESTIC_PICKUP_PENDING'
    when t1.parcel_logistics_status_code in (8000,8040) then 'RETURN_STARTED'
    when t1.parcel_logistics_status_code = 4101 then 'UPDATE_STORE_SUBMITTED'
    when t1.parcel_logistics_status_code = 4103 then 'UPDATE_STORE_CANCEL'
    when t1.parcel_logistics_status_code = 9994 then 'IMLM_DISPOSED'
    when t1.parcel_logistics_status_code = 8060 then 'PARCEL_CONFISCATED'
    when t1.parcel_logistics_status_code = 9993 then 'TERMINATE_AS_DAMAGE'
    else 'no'
  end as sls_status
   FROM sls_mart.dwd_ofg_forder_union_df_vn t3
    left join sls_mart.dwd_parcel_detail_union_nonsensitive_v2_df_vn t1 on t1.grass_date = current_date - interval '1' day and t1.log_id = t3.log_id
   LEFT JOIN pku_done_tracking t5 ON (t5.log_id = t3.log_id)
   left join sls_mart.shopee_ssc_lfs_order_vn_db__logistic_order_data_tab__reg_continuous_s0_live rd on  rd.slo_id = t3.log_id

)
SELECT
  case 
  when date(scheduled_date1) between date '2023-02-02' and date '2023-02-04' then '2/2'
  when date(scheduled_date1) between date '2023-02-15' and date '2023-02-17' then '15/2'
  when date(scheduled_date1) between date '2023-03-03' and date '2023-03-05' then '3/3'
  else 'no' end as "CP"
, o.fulfilment_shipping_carrier
--, f.lm_tracking_no
--, o.order_sn
--, o.seller_shipping_address_state
--, o.seller_shipping_address_city
--, o.seller_shipping_address_district
--, o.cancel_reason
--, o.buyer_cancel_reason
--, o.logistics_status
--, o.order_be_status
--, f.sls_status
--, o.shop_id
--, o.shop_name
--, o.buyer_id
--, f.scheduled_date1
--, f.pku_1st_time
--, date(f.scheduled_date1)
--, coalesce(f.shipment_type,f.shipment_type1) shipment_type

, sum(gross_pickup_request) gross_pickup
, sum(gross_pickup_done) gross_pickup_done
, sum(gross_pickup_failed) gross_pickup_failed
, sum(ontime_pickup_done) gross_ontime_PU_done
, sum(ontime_pickup) gross_ontime_PU
--, round(approx_percentile(((to_unixtime(pku_done_time) - to_unixtime(pku_1st_time))/86400),0.95),3) gap_time
-- , cast(sum(gross_pickup_done) as DOUBLE)/ sum(gross_pickup_request) rate_pickup_done
-- , avg(pickup_leadtime) avg_pickup_leadtime

FROM (select * from mp_order.dwd_order_all_ent_df__vn_s0_live where grass_date > date '2022-07-01') o 
left join Final f on f.order_id = o.order_id
WHERE o.fulfilment_shipping_carrier in ('VNPost Nhanh','VNPost Tiết Kiệm')
and f.shipment_type1 = 'pickup'
and (
    date(scheduled_date1) between date '2023-02-02' and date '2023-02-04'
    or date(scheduled_date1) between date '2023-02-15' and date '2023-02-17'
    or date(scheduled_date1) between date '2023-03-03' and date '2023-03-05')
GROUP BY 1,2
