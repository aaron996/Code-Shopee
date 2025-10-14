WITH
multi AS
(
    Select
    log_id,
    count(distinct lm_tracking_no) as nb_parcels
    FROM (select * from sls_mart.dwd_parcel_detail_union_nonsensitive_df_vn where grass_date > date '2021-01-01')
    where whs_id is not null
    GROUP by 1
    HAVING count(distinct lm_tracking_no) > 1
)
,group_shipment AS
(
SELECT
    order_id,
    count(oms_forder_id) As total_group
FROM
    (
    Select
            order_id,
            oms_forder_id
           -- max(fulfilment_end_type)
    FROM (select * from sls_mart.dwd_ofg_forder_union_df_vn  where grass_date = current_date - interval '1' day)
    )
GROUP by 1
)
,pku_failed as
(
    select slo_id, min(from_unixtime(actual_time-3600)) as tracking_time
    from sls_mart.shopee_ssc_lts_tracking_vn_db__logistic_tracking_tab__reg_continuous_s0_live
    where tracking_code in ('F097')
    group by 1
)
,pku_done as
(
    select slo_id, min(from_unixtime(actual_time-3600)) as tracking_time
    from sls_mart.shopee_ssc_lts_tracking_vn_db__logistic_tracking_tab__reg_continuous_s0_live
    where tracking_code in ('F100','F510')
    group by 1
)
, dli_done as
(
    select log_id, min(from_unixtime(latest_update_timestamp-3600)) as tracking_time
    from (SELECT * FROM sls_mart.dwd_parcel_detail_union_df_vn where grass_date = current_date - interval '2' day)
    where log_status = 11
    group by 1
)

, dli_failed as
(
    select log_id, min(from_unixtime(latest_update_timestamp-3600)) as tracking_time
    from (SELECT * FROM sls_mart.dwd_parcel_detail_union_df_vn where grass_date = current_date - interval '2' day)
    where log_status in (12,20)
    group by 1
)
,return as
(
    SELECT * FROM(
        select distinct slo_id,
                from_unixtime(actual_time-3600) as tracking_time,
                json_extract_scalar(tracking_detail,'$.reason') tracking_detail_reason,
                rank() over (partition by slo_id order by actual_time) AS rnk
            from sls_mart.shopee_ssc_lts_tracking_vn_db__logistic_tracking_tab__reg_continuous_s0_live
                where 
                    -- channel_status in ('On Vehicle for Delivery')
                    resource_status in ('25') 
    ) where rnk = 1
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
--,o.seller_shipping_address_state as seller_state
--,o.seller_shipping_address_city AS seller_city
--,o.seller_shipping_address_district AS seller_district
--,o.buyer_shipping_address_state as buyer_state
--,o.buyer_shipping_address_city as buyer_city
--,o.buyer_shipping_address_district as buyer_district
--,t2.deliver_address
--,o.buyer_id
--,o.buyer_name
,o.seller_id
,o.shop_id
--,case when o.is_preferred_shop = 1 then 'preferred_shop' 
--when o.is_preferred_plus_shop = 1 then 'preferred_plus_shop' 
--when o.is_official_shop = 1 then 'shop Mall' 
--else 'no' 
--end as seller_type
,o.shop_name
,o.fulfilment_shipping_carrier
--,(CASE WHEN year(from_unixtime(rd.pickup_time-3600)) = 1970 THEN 'drop off' else 'pick up' END) as shipment_type
--,case when multi.nb_parcels IS NOT NULL then 'Multi WH' else 'Not Multi WH' end as "Check_Multi_WH"
--,o.escrow_to_seller_amt escrow_to_seller
--,o.gmv grand_total
--,t1.items_sales_value_local cogs_parcel
--,t1.chargeable_weight parcel_actual_weight
--,from_unixtime(t3.scheduled_pickup_timestamp-3600) arrange_pickup
--,o.auto_cancel_arrange_ship_datetime ACL1_date
--,date(from_unixtime(o.auto_cancel_3pl_ack_timestamp - 3600)) ACL2_date
--,from_unixtime(o.create_timestamp - 3600) Order_created
--,from_unixtime(t1.order_arrange_shipping_timestamp - 3600) Logistics_request_created
--,o.cancel_datetime cancel_timestamp
--,pd.tracking_time pickup_done
--,t3.fm_pickup_failed_datetime pickup_failed
--,ri.tracking_time return_initiated
--,(case when o.payment_method = 'PAY_COD' then 'COD'
--else 'CC' end) as payment_method
--,group_shipment.total_group "group shipment"

FROM (SELECT * from mp_order.dwd_order_all_ent_df__vn_s0_live where grass_date >= date'2021-01-01' and grass_region = 'VN') o
LEFT JOIN sls_mart.shopee_ssc_lfs_order_vn_db__logistic_order_tab__vn_continuous_s0_live r on o.order_sn = r.ordersn
LEFT JOIN sls_mart.shopee_ssc_lfs_order_vn_db__logistic_order_data_tab__vn_continuous_s0_live rd on rd.slo_id = r.slo_id
--LEFT JOIN (SELECT * FROM sls_mart.dwd_ofg_forder_union_df_vn where grass_date = current_date - interval '1' day and grass_region = 'VN') t1 on t1.log_id = r.slo_id
--LEFT JOIN return ri on ri.slo_id = r.slo_id
LEFT JOIN (SELECT * FROM sls_mart.dwd_parcel_detail_union_nonsensitive_df_vn where grass_date = current_date - interval '1' day and grass_region = 'VN') t3 on t3.log_id = r.slo_id
--left join multi on multi.log_id = r.slo_id
--Left join group_shipment on group_shipment.order_id = o.order_id
--left join pku_done pd on pd.slo_id = r.slo_id
--left join pku_failed pf on pf.slo_id = r.slo_id
--left join dli_done dd on dd.log_id = r.slo_id
--left join sls_mart.shopee_ssc_lfs_order_vn_db__logistic_order_data_tab__reg_continuous_s0_live t2 on t2.slo_id = r.slo_id

where 
--o.shop_id = 45097318
--o.seller_id = 141860611 or o.shop_id = 141860611
--and o.logistics_status_id = 1
o.fulfilment_shipping_carrier in ('VNPost Nhanh','VNPost Tiết Kiệm')
--and o.logistics_status_id = 11
--and pd.tracking_time between date '2022-04-01' and date '2022-08-03'
--o.shop_name in ('qingqi.vn')
and o.logistics_status_id = 9
and date(o.auto_cancel_arrange_ship_datetime) = date '2022-10-14'

--limit 100
