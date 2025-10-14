WITH multi as
(
    select cast(order_id as bigint) order_id, count(distinct forder_id) nb_parcels
    from oms_mart.shopee_oms_vn_db__fulfillment_order_tab__vn_daily_s0_live
    where whs_id is not null
    group by 1
    having count(distinct forder_id) > 1
)
,sls_tracking as
(select 
   distinct sls.log_id,
   min(case when sls.status = 1 then from_unixtime(sls.update_time-3600) else null end) as sls_received_time,
   min(case when sls.status in (26,2,8,9) then from_unixtime(sls.update_time-3600) else null end) as sls_first_pickup_attempt,
   min(case when sls.status in (2,8,9) then from_unixtime(sls.update_time-3600) else null end) as sls_pickup_done,
--    min(case when sls.channel_status in ('503') then from_unixtime(sls.update_time-3600) else null end) as sls_time_503,
   min(case when sls.status in (11) then from_unixtime(sls.update_time-3600) else null end) as sls_delivery_done,
   min(case when sls.status in (18,22) then from_unixtime(sls.update_time-3600) else null end) as sls_return_time,
   max(case when sls.status in (12,19,20) then from_unixtime(sls.update_time-3600) else null end) as sls_del_failed,
   max(case when sls.status in (13) then from_unixtime(sls.update_time-3600) else null end) as sls_del_pending_sub   
    from sls_mart.shopee_sls_logistic_vn_db__logistic_tracking_tab_lfs_union_tmp sls 
    left join sls_mart.dwd_forder_all_ent_df__vn_s0_live f on sls.log_id = f.log_id
    where sls.grass_region = 'VN'
    and sls.grass_date = current_date - interval '1' day
        and f.actual_fulfilment_channel_id = 50016
        and DATE(from_unixtime(f.order_create_timestamp - 3600)) >= date_trunc('month',current_date ) - interval '3' month --DATE('2021-04-01')
    group by 1
)
,sls_first_delivery_pending as
(
    SELECT 
    t1.log_id, 
MIN(t1.sls_del_pending_sub) AS sls_first_delivery_pending 
FROM sls_tracking as t1
    WHERE t1.sls_del_pending_sub between sls_pickup_done AND coalesce (sls_return_time,sls_delivery_done)
    GROUP BY 1)
,sls_first_delivery_attempt AS
(SELECT 
case when t1.log_id is null then t2.log_id else t1.log_id end as log_id
                ,coalesce(sls_first_delivery_pending,t2.sls_delivery_done) as "sls_first_delivery_attempt"
from sls_first_delivery_pending as t1
left join sls_tracking as t2
                on t1.log_id = t2.log_id
)
,sls_return_pending AS
(SELECT t1.log_id, MIN(sls_del_pending_sub) AS sls_return_pending FROM sls_tracking as t1
        WHERE sls_del_pending_sub > sls_return_time AND sls_del_pending_sub < sls_del_failed
        GROUP BY 1
)
,sls_first_return_attempt as
(select 
t1.log_id, 
coalesce(sls_return_pending,sls_del_failed) as sls_first_return_attempt 
from sls_return_pending as t1
        left join sls_tracking as t2
        on t1.log_id = t2.log_id
)
,log_request_tab as
(select log_id,ordersn,lm_tracking_number
    from sls_mart.shopee_sls_logistic_vn_db__logistic_request_tab_lfs_union_tmp where grass_region = 'VN' and grass_date = current_date - interval '1' day)
,base_sls as
(select t0.log_id
                ,sls_received_time
                ,sls_first_pickup_attempt
                ,sls_pickup_done
                ,sls_first_delivery_pending
                ,sls_first_delivery_attempt
                ,sls_delivery_done
                ,sls_return_time
                ,sls_del_failed
                ,sls_return_pending
                ,sls_first_return_attempt

                from sls_tracking as t0
                left join sls_first_delivery_pending as t3
                        on t0.log_id = t3.log_id
                left join sls_first_delivery_attempt as t4
                        on t0.log_id = t4.log_id
                left join sls_return_pending as t8
                        on t0.log_id = t8.log_id
                left join sls_first_return_attempt as t9
                        on t0.log_id = t9.log_id
)
,delivery as
(select  order_id orderid
                ,rt.log_id
                ,oo.order_sn ordersn
                ,rt.lm_tracking_number shipping_traceno
                ,oo.fulfilment_shipping_carrier
                ,rt.lm_tracking_number
                ,case when oo.logistics_status_id in (5) then from_unixtime(oo.complete_timestamp) else null end as "be__delivered_time" ,case
                when cancel_reason in ('CANCEL_REASON_LOGISTICS_DELIVERY_FAILED','CANCEL_REASON_LOST_PARCEL') then from_unixtime(oo.cancel_timestamp) else null end as  "be__delivery_failed_lost_time" 
        ,oo.logistics_status
                ,sls_received_time
                ,sls_first_pickup_attempt
                ,sls_pickup_done
                ,sls_first_delivery_pending
                ,sls_first_delivery_attempt
                ,sls_delivery_done
                ,sls_return_time
                ,sls_del_failed
                ,sls_return_pending
                ,sls_first_return_attempt
                ,from_unixtime(rd.pickup_time -3600)  fe_schedule_pickup ,CASE WHEN year("from_unixtime"((rd.pickup_time - 3600))) > 1970 THEN 0 else 1 end is_dropoff
            ,case when hour(sls_pickup_done) < 12 then date_trunc('day',(sls_pickup_done)) + interval '12' hour else date_trunc('day',(sls_pickup_done)) + interval '1' day end as adjusted_pickup
                ,case when hour(sls_return_time) < 12 then date_trunc('day',(sls_return_time)) + interval '12' hour else date_trunc('day',(sls_return_time)) + interval '1' day end as adjusted_return

                from mp_order.dwd_order_all_ent_df__vn_s0_live  oo
                left join sls_mart.shopee_sls_logistic_vn_db__logistic_request_tab_lfs_union_tmp as rt
                        on oo.order_sn = rt.ordersn and rt.grass_region = 'VN' and rt.grass_date = current_date - interval '1' day
                left join base_sls as sls
                        on rt.log_id = sls.log_id
                left join sls_mart.shopee_sls_logistic_vn_db__logistic_request_data_tab_lfs_union_tmp rd on rd.grass_region = 'VN' and rd.grass_date = current_date - interval '1' day and rd.log_id = rt.log_id

                where oo.is_cb_shop = 0 and oo.grass_date > date '2021-01-01'
                and oo.fulfilment_channel_id = 50016  )

,base1 as
(
    select *

    from

(    select distinct
      dd.orderid
     ,case when m.order_id is not null then 1 else 0 end is_multi_parcel
     ,sls.consignment_no as consignment
     ,dd.ordersn
     ,dd.shipping_traceno
     ,dd.log_id
     ,dd.fulfilment_shipping_carrier
     ,o.shop_id shopid
     ,o.seller_id seller_userid
     ,o.buyer_id userid
     ,dd.lm_tracking_number
     ,o.buyer_paid_shipping_fee be_bpsf
     ,o.estimate_shipping_fee be_esf
     ,o.actual_shipping_fee be_asf

     ,sls.actual_shipping_fee sls_asf

     ,cast(oms.origin_shipping_fee as double)/100000 sls_esf
     ,case when m.order_id is null then o.seller_shipping_address_state else w.state end seller_address_state
     ,case when m.order_id is null then o.seller_shipping_address_city else w.city end seller_address_city
     ,case when m.order_id is null then o.seller_shipping_address_district else w.district end seller_address_district
     ,o.buyer_shipping_address_state buyer_address_state
     ,o.buyer_shipping_address_city buyer_address_city
     ,o.buyer_shipping_address_district buyer_address_district
     ,from_unixtime(oms.arranged_time-3600) multi_parcel__arranged_time
     ,from_unixtime(oms.delivered_time) multi_parcel__delivered_time
     ,cast(json_extract_scalar(oms.attributes,'$.chargeable_weight') as double)/1000 order_weight
     ,dd.be__delivered_time ,dd.be__delivery_failed_lost_time ,dd.logistics_status
     ,dd.sls_received_time
         ,dd.sls_first_pickup_attempt
         ,dd.sls_pickup_done
         ,dd.sls_first_delivery_pending
         ,dd.sls_first_delivery_attempt
         ,dd.sls_delivery_done
         ,dd.sls_return_time
         ,dd.sls_del_failed
         ,dd.sls_return_pending
         ,dd.sls_first_return_attempt
         ,dd.fe_schedule_pickup
         ,dd.is_dropoff
         ,dd.adjusted_pickup
         ,dd.adjusted_return
     from delivery dd
    join mp_order.dwd_order_all_ent_df__vn_s0_live o
        on dd.orderid = o.order_id
    left join multi m
        on m.order_id = o.order_id
    left join sls_mart.shopee_sls_logistic_vn_db__logistic_request_tab_lfs_union_tmp sls
        on sls.ordersn = o.order_sn and sls.log_id = dd.log_id and sls.grass_region = 'VN' and sls.grass_date = current_date - interval '1' day
    left join oms_mart.shopee_oms_vn_db__fulfillment_order_tab__vn_daily_s0_live oms
        on cast(oms.forder_id as varchar) = cast(sls.forderid as varchar)
    left join oms_mart.shopee_oms_vn_db__warehouse_tab__vn_daily_s0_live w
        on oms.whs_id = w.whs_id
 where (
                (date(dd.be__delivered_time) between date '2022-06-01' and date '2022-06-30')
        or (date(dd.be__delivery_failed_lost_time) between date '2022-06-01' and date '2022-06-30')
        )

) as base

)

,tab_temp as
(select *
                ,case when date(sls_first_pickup_attempt) > date (fe_schedule_pickup) and is_dropoff = 0 then '50% trễ lấy-' else '' end is_delay_pickup
                ,(b.total_sla + b.delivery_w_weekend + b.nb_delivery_holiday) as limit_leadtime_delivery
                ,(b.total_sla + b.return_w_weekend + b.nb_return_holiday) as limit_leadtime_return
                , (15 + nb_delivery_holiday + added_leadtime) as total_leadtime_toantrinh_giao
                , (15 + nb_return_holiday + added_leadtime) as total_leadtime_toantrinh_hoan

from
(
        select  b.*
                        ,r.ship_from
                        ,r.ship_to
                        ,r.sla sla
                        ,sr.region seller_region
                        ,br.region buyer_region

                        ,cast(r.sla as double) as total_sla
                        ,case when cast(eco_remote.added_leadtime as int) is not null then cast(eco_remote.added_leadtime as int) else 0 end as added_leadtime

                        ,cast(day_of_week(b.adjusted_pickup) as double) +1 as "day_of_week_del"
                        ,floor(
                          (cast(day_of_week(b.adjusted_pickup) as double)
                        + cast(hour(b.adjusted_pickup) as double)/24
                        + cast(r.sla as double) )/7.1) as "delivery_w_weekend" ,cast(date_diff('second',adjusted_pickup,coalesce(sls_first_delivery_attempt,sls_delivery_done)) as double)/3600/24 as del_leadtime
                        ,cast(date_diff('second',adjusted_pickup,coalesce(sls_return_time,sls_delivery_done)) as double)/3600/24 as del_leadtime_toantrinh

                        ,cast(day_of_week(b.adjusted_return) as double) +1 as "day_of_week_return"
                        ,floor(
                          (cast(day_of_week(b.adjusted_return) as double)
                        + cast(hour(b.adjusted_return) as double)/24
                        + cast(r.sla as double) )/7.1) as "return_w_weekend" ,cast(date_diff('second',adjusted_return,coalesce(sls_first_return_attempt,sls_del_failed)) as double)/3600/24 as return_leadtime
                        ,cast(date_diff('second',adjusted_return,sls_del_failed) as double)/3600/24 as return_leadtime_toantrinh

                        ,coalesce(cast(d.nb_holiday as int),0) nb_delivery_holiday
                        ,coalesce(cast(t.nb_holiday as int),0) nb_return_holiday
                        ,coalesce(cast(d_o.nb_holiday as int),0) nb_delivery_holiday_overall
                        ,coalesce(cast(t_o.nb_holiday as int),0) nb_return_holiday_overall
        from base1 as b
                left join vnbi_ops.vnpeco_region_  br
                        on lower(b.buyer_address_state) = br.state and br.ingestion_timestamp = (select max(ingestion_timestamp) it from vnbi_ops.vnpeco_region_)
                left join vnbi_ops.vnpeco_region_  sr
                        on lower(b.seller_address_state) = sr.state and sr.ingestion_timestamp = (select max(ingestion_timestamp) it from vnbi_ops.vnpeco_region_)
                left join vnbi_ops.vnpeco_route r
                        on sr.region = r.ship_from and br.region = r.ship_to and r.ingestion_timestamp = (select max(ingestion_timestamp) it  from vnbi_ops.vnpeco_route)
                left join vnbi_ops.shopee_vn_op_team__db_tpl_holiday d
                        on b.fulfilment_shipping_carrier = d.shipping_carrier and d.shipping_carrier = 'VNPost Tiết Kiệm' and d.type = 'delivery' and (cast(d.holiday_start as date) between date(adjusted_pickup) and date_add ('day',cast(r.sla as int),date(adjusted_pickup)) ) and d.ingestion_timestamp = (select max(ingestion_timestamp) it from vnbi_ops.shopee_vn_op_team__db_tpl_holiday)
                left join vnbi_ops.shopee_vn_op_team__db_tpl_holiday t
            on b.fulfilment_shipping_carrier = t.shipping_carrier and t.shipping_carrier = 'VNPost Tiết Kiệm' and t.type = 'return' and (cast(t.holiday_start as date) between date(adjusted_return) and date_add ('day',cast(r.sla as int),date(adjusted_return)) ) and t.ingestion_timestamp = (select max(ingestion_timestamp) it from vnbi_ops.shopee_vn_op_team__db_tpl_holiday)
                left join vnbi_ops.shopee_vn_op_team__db_tpl_holiday d_o
            on b.fulfilment_shipping_carrier = d_o.shipping_carrier and d_o.shipping_carrier = 'VNPost Tiết Kiệm' and d_o.type = 'delivery' and (cast(d_o.holiday_start as date) between date(adjusted_pickup) and date_add ('day',15,date(adjusted_pickup)) ) and d_o.ingestion_timestamp = (select max(ingestion_timestamp) it from vnbi_ops.shopee_vn_op_team__db_tpl_holiday)
                left join vnbi_ops.shopee_vn_op_team__db_tpl_holiday t_o
            on b.fulfilment_shipping_carrier = t_o.shipping_carrier and t_o.shipping_carrier = 'VNPost Tiết Kiệm' and t_o.type = 'return' and (cast(t_o.holiday_start as date) between date(adjusted_return) and date_add ('day',15,date(adjusted_return)) ) and t_o.ingestion_timestamp = (select max(ingestion_timestamp) it from vnbi_ops.shopee_vn_op_team__db_tpl_holiday)
                left join vnbi_ops.vnpeco_remote_area as eco_remote
                        on eco_remote.buyer_address = lower(concat(b.buyer_address_state,'-',b.buyer_address_city)) and eco_remote.ingestion_timestamp = (select max(ingestion_timestamp) it  from vnbi_ops.vnpeco_remote_area)
                ) as b
)

,t as
(select distinct tab_temp.*
                ,case when del_leadtime > limit_leadtime_delivery then '50% trễ giao-' else '' end as "is_delay_delivery"
                ,case when return_leadtime > limit_leadtime_return then '50% trễ hoàn-' else '' end as "is_delay_return"
                ,case when del_leadtime_toantrinh > (15+ nb_delivery_holiday_overall + added_leadtime)  then '100% trễ giao-' else '' end as is_delay_delivery_overall
                ,case when return_leadtime_toantrinh > (15+ nb_return_holiday_overall + added_leadtime)  then '100% trễ hoàn-' else '' end as is_delay_return_overall
                from tab_temp
)

select distinct
                 t.orderid
                ,t.is_multi_parcel
                ,t.consignment
                ,t.ordersn
                ,t.shipping_traceno
                ,t.lm_tracking_number
                ,t.log_id
                ,t.fulfilment_shipping_carrier
                ,t.shopid
                ,t.seller_userid
                ,t.userid
                ,t.be_bpsf
                ,t.be_esf
                ,t.be_asf
                ,t.sls_asf
                ,t.sls_esf
                ,t.seller_address_state
                ,t.seller_address_city
                ,t.seller_address_district
                ,t.buyer_address_state
                ,t.buyer_address_city
                ,t.buyer_address_district
                ,t.multi_parcel__arranged_time
                ,t.multi_parcel__delivered_time
                ,t.order_weight
                ,t.be__delivered_time
                ,t.be__delivery_failed_lost_time
                ,t.seller_region
                ,t.buyer_region
                ,t.day_of_week_del
                ,t.day_of_week_return
                ,t.logistics_status
                ,t.added_leadtime
                ,t.ship_from
                ,t.ship_to
                ,t.sls_first_return_attempt
                ,t.sls_first_delivery_attempt
                ,t.total_sla
                ,t.is_dropoff
                ,t.sls_received_time
                ,t.fe_schedule_pickup
                ,t.sls_first_pickup_attempt
                ,t.sls_pickup_done
                ,t.adjusted_pickup
                ,t.sls_first_delivery_pending
                ,t.sls_delivery_done
                ,t.sls_return_time
                ,t.adjusted_return
                ,t.sls_return_pending
                ,t.sls_del_failed
                ,t.sla
                ,0 as pickup
                ,0 as inter
                ,0 as intra
                ,t.delivery_w_weekend
                ,t.nb_delivery_holiday
                ,0 as disaster
                ,t.return_w_weekend
                ,t.nb_return_holiday
                ,t.limit_leadtime_delivery
                ,t.del_leadtime
                ,t.total_leadtime_toantrinh_giao
                ,t.del_leadtime_toantrinh
                ,t.limit_leadtime_return
                ,t.return_leadtime
                ,t.total_leadtime_toantrinh_hoan
                ,t.return_leadtime_toantrinh
                ,t.is_delay_pickup
                ,t.is_delay_delivery
                ,t.is_delay_delivery_overall
                ,t.is_delay_return
                ,t.is_delay_return_overall
                ,concat(is_delay_pickup,is_final_delay_delivery,is_final_delay_return) as final_penalty
from
(select t.*
                ,case when is_delay_delivery_overall = '100% trễ giao-' then is_delay_delivery_overall else is_delay_delivery end as is_final_delay_delivery
                ,case when is_delay_return_overall = '100% trễ hoàn-' then is_delay_return_overall else is_delay_return end as is_final_delay_return
        from t
                where         is_delay_pickup = '50% trễ lấy-'
                                or is_delay_delivery = '50% trễ giao-'
                                or is_delay_return = '50% trễ hoàn-'
                                or is_delay_delivery_overall = '100% trễ giao-'
                                or is_delay_return_overall = '100% trễ hoàn-'
                                ) as t
