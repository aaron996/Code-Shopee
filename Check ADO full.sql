with ado as 
(
select 
seller_shipping_address_state,
seller_shipping_address_city,
buyer_shipping_address_state,
buyer_shipping_address_city
from (SELECT * from mp_order.dwd_order_all_ent_df__vn_s0_live where grass_date >= date'2023-01-01' and grass_region = 'VN')
where 
    (seller_shipping_address_state in ('Quảng Nam') and seller_shipping_address_city in ('Thành Phố Hội An'))
or  (buyer_shipping_address_state in ('Quảng Nam') and buyer_shipping_address_city in ('Thành Phố Hội An'))
)
SELECT 
week(from_unixtime(o.create_timestamp-3600)) week_,
--r.lm_tracking_number
--,o.Logistics_Status
--,o.fulfilment_shipping_carrier
case          
when o.fulfilment_channel_id  =    50024   then    'BEST Express'
when o.fulfilment_channel_id  =    50021   then    'Shopee Express'
when o.fulfilment_channel_id  =    50018   then    'J&T Express'
when o.fulfilment_channel_id  =    50010   then    'Viettel Post'
-- when t2.fulfilment_channel_id  =    50011   then    'Giao Hàng Nhanh'
when o.fulfilment_channel_id  =    50012   then    'Giao Hàng Tiết Kiệm'
when o.fulfilment_channel_id  =    50015   then    'VNPost Nhanh'
--when o.fulfilment_channel_id  =    50023   then    'Ninja Van'
when o.fulfilment_channel_id  =    50016   then    'VNPost Tiết Kiệm'
when o.fulfilment_channel_id  =    5002    then    'Tiết kiệm'
when o.fulfilment_channel_id = 50011 and o.shipping_method_id  in (5001)  then 'Giao Hàng Nhanh'
when o.fulfilment_channel_id = 50011 and o.shipping_method_id  in (5002)  then 'Giao Hàng Nhanh - ECO'
when o.fulfilment_channel_id = 50023 and o.shipping_method_id  in (5001)   then  'Ninja Van'
when o.fulfilment_channel_id = 50023 and o.shipping_method_id  in (5002)  then 'Ninja Van - ECO'

else  'no' end "3PL name"

,sum(
    case when (o.seller_shipping_address_state = a.seller_shipping_address_state
        and o.seller_shipping_address_city = a.seller_shipping_address_city) then 1 else 0 end) as "Pending pickup"
,sum(
    case when (o.buyer_shipping_address_state = a.buyer_shipping_address_state
        and o.buyer_shipping_address_city = a.buyer_shipping_address_city) then 1 else 0 end) as "Pending deli"

--seller_shipping_address_city,
--seller_shipping_address_district,
--,count(distinct order_sn) as volume
--,count(distinct o.order_sn)

FROM (SELECT * from mp_order.dwd_order_all_ent_df__vn_s0_live where grass_date >= date'2023-01-01' and grass_region = 'VN') o
left join sls_mart.shopee_ssc_lfs_order_vn_db__logistic_order_tab__vn_continuous_s0_live r on o.order_sn = r.ordersn
left join ado a on a.seller_shipping_address_state = o.seller_shipping_address_state
                and a.seller_shipping_address_city = o.seller_shipping_address_city
                and a.buyer_shipping_address_state = o.buyer_shipping_address_state
                and a.buyer_shipping_address_city = o.buyer_shipping_address_city
Where 

date(from_unixtime(o.create_timestamp-3600)) between (current_date - interval '60' day) and current_date


group by 1,2
