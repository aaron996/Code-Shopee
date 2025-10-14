SELECT 
concat(cast(month(date(from_unixtime(o.create_timestamp-3600)))as varchar ),'/',cast(year(date(from_unixtime(o.create_timestamp-3600)))as varchar )) month_
,i.global_be_category g_cate 
    ,count(distinct o.order_sn) nb_order
    ,sum((case when o.payment_method = 'PAY_COD' then 1 else 0 end)) cod_order
    ,sum((case when o.payment_method not in ('PAY_COD') then 1 else 0 end)) cc_order

FROM (SELECT * from mp_order.dwd_order_all_ent_df__vn_s0_live where grass_date >= date'2020-01-01' and grass_region = 'VN') o
left join (select * from mp_order.dwd_order_item_all_ent_df__vn_s0_live where grass_date >= date'2020-01-01' and grass_region = 'VN') i on i.order_id = o.order_id
--left join (select * from mp_user.dim_user__vn_s0_live where grass_region = 'VN' and grass_date >= date '2022-01-01') m on m.user_id = o.buyer_id 

where 

date(from_unixtime(o.create_timestamp-3600)) between date '2020-01-01' and date '2022-12-31'
--and o.fulfilment_shipping_carrier in ('VNPost Nhanh','VNPost Tiết Kiệm')
group by  1,2
