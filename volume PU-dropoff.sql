SELECT 
    month(date(from_unixtime(t3.scheduled_pickup_timestamp-3600))) month
    ,o.fulfilment_shipping_carrier
    ,count(distinct o.order_sn) nb_order

FROM (SELECT * from mp_order.dwd_order_all_ent_df__vn_s0_live where grass_date >= date'2022-01-01' and grass_region = 'VN') o
LEFT JOIN sls_mart.shopee_ssc_lfs_order_vn_db__logistic_order_tab__vn_continuous_s0_live r on o.order_sn = r.ordersn
LEFT JOIN (SELECT * FROM sls_mart.dwd_parcel_detail_union_nonsensitive_df_vn where grass_date = current_date - interval '1' day and grass_region = 'VN') t3 on t3.log_id = r.slo_id

where 


date(from_unixtime(t3.scheduled_pickup_timestamp-3600)) between date '2022-05-01' and date '2022-10-31'
and o.fulfilment_shipping_carrier in ('VNPost Nhanh','VNPost Tiết Kiệm')
group by  1,2
