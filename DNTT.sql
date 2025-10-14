WITH DNTT AS
(
SELECT
    distinct t1.order_id orderid,
    t1.order_sn ordersn,
    t2.consignment_no,
    (from_unixtime(t1.create_timestamp-3600)) AS created_date,
    t1.order_be_status status,
 t1.logistics_status Shopee_Logistics_Status,
    t1.order_be_status Order_status,
    (CASE
        WHEN t1.payment_method_id = 6 THEN 'COD'
        ELSE 'CC'
    END) AS Payment_method,
    t2.lm_tracking_no shipping_traceno,
    t1.shipping_carrier,
    t1.payment_channel,
    ob.username as buyer_name,
    t1.buyer_id as buyer_userid,
    om.userid as seller_userid,
    om.username as seller_user_name,
    t3.items_sales_value_local cogs,
    otp.cogs_item,
    t1.gmv as COD_amount,
    t1.escrow_to_seller_amt escrow_to_seller,
    otp.comm_fee as Commission_Fee,
    t1.seller_txn_fee as seller_transaction_fee_order_mart
    ,otp.seller_transaction_fee as seller_transaction_fee_order_item
    ,case when payment_be_channel_id in (51000 , 50020, 50021) then 0
          when payment_be_channel_id not in (51000 , 50020, 50021) and  (date(from_unixtime(t1.create_timestamp-3600)) < date '2022-04-01') then ROUND(t1.gmv*0.022,0)
          when payment_be_channel_id not in (51000 , 50020, 50021) and  (date(from_unixtime(t1.create_timestamp-3600)) >= date '2022-04-01') then ROUND(t1.gmv*0.025,0)
          end as  Seller_transaction_fee
    ,otp.service_fee as Service_fee,
    (sv_rebate_by_seller_amt + pv_rebate_by_seller_amt) as seller_voucher,
    t1.actual_shipping_rebate_by_seller_amt as seller_shipping_rebate,
    t1.buyer_paid_shipping_fee as T1_buyer_shipping_fee,
    t1.card_rebate_by_bank_amt as T2_bank_card_rebate,
    (sv_rebate_by_seller_amt + pv_rebate_by_seller_amt) as T3_seller_voucher_rebate,
    t1.card_rebate_by_shopee_amt as T4_shopee_card_rebate,
    t1.coin_used_cash_amt as T5_shopee_coin_rebate,
    (sv_rebate_by_shopee_amt + pv_rebate_by_shopee_amt) T6_shopee_voucher_rebate,
    t1.estimate_shipping_fee as ESF,
    t1.actual_shipping_fee as ASF,
    t1.actual_shipping_rebate_by_shopee_amt shopee_actual_shipping_rebate ,
    t1.estimate_shipping_rebate_by_shopee_amt,
    sv_coin_earn_by_seller_amt as  coin_earned_shop,
    (sv_coin_earn_by_shopee + pv_coin_earn_by_shopee + sv_coin_earn_by_seller + pv_coin_earn_by_seller)*100 coin_earn_by_voucher,
    t1.gmv*0.02 as Grand_total_20,
    t1.gmv*0.022 as Grand_total_22,
    payment_be_channel,
    t1.coin_used*100 Buyer_Coins_Spent,
    t1.pv_rebate_by_shopee_amt,
    se.buyer_paid_insurance_premium,
    ins.premium,
    ins.premium_after_discount,
    otp.insurance_premium_by_buyer_amt


    FROM mp_order.dwd_order_all_ent_df__vn_s0_live  as t1
left join sls_mart.dwd_ofg_forder_union_df_vn t3 on t3.grass_date = current_date - interval '1' day and t3.order_id = t1.order_id
left join sls_mart.dwd_parcel_detail_union_nonsensitive_v2_df_vn t2 on t2.grass_date = current_date - interval '1' day and t2.log_id = t3.log_id
LEFT JOIN marketplace.shopee_account_v2_db__account_tab__vn_daily_s0_live as om
    ON t1.shop_id = om.shopid
LEFT JOIN marketplace.shopee_account_v2_db__account_tab__vn_daily_s0_live as ob
    ON t1.buyer_id = ob.userid
LEFT JOIN
    (SELECT
   order_id orderid,
    sum(commission_fee) as comm_fee
    ,sum(seller_txn_fee) as seller_transaction_fee
    ,sum(service_fee) as service_fee
    ,sum(cogs) as cogs_item
    , sum(insurance_premium_by_buyer_amt) insurance_premium_by_buyer_amt

    from  mp_order.dwd_order_item_all_ent_df__vn_s0_live
    where grass_date > date '2021-01-01'
    group by 1
    )as otp
    on t1.order_id = otp.orderid
Left join seafin_shopee.seafin_shopee_vn_escrow_created se on cast(se.order_id as bigint) = t1.order_id
Left join insurance.dwd_insurance_vn_policy_df ins on cast(ins.partner_order_id as bigint) = t1.order_id
where t1.grass_date > date '2021-01-01'
)

,multi AS
(select cast(order_id as bigint) order_id, count(distinct consignment_no) nb_parcels
    from sls_mart.dwd_ofg_forder_union_df_vn
    where grass_date = current_date - interval '1' day
    and is_reverse = 0
    group by 1
    having count(distinct consignment_no) > 1 
)
,group_shipment AS
(
    select  lm_tracking_no
            ,count(distinct ofg_id) as total_group
    from    (
        select  of.lm_tracking_no
                ,t3.ofg_id
                --,fulfilment_end_type
                --,max(fulfilment_end_type) as max_fulfilment_end_type
        from    sls_mart.dwd_parcel_detail_union_nonsensitive_v2_df_vn of
        left join sls_mart.dwd_ofg_forder_union_df_vn t3 on t3.grass_date = current_date - interval '1' day and t3.log_id = of.log_id
        left join sls_mart.dwd_order_detail_info_nonsensitive_df_vn t1 on t1.grass_date = current_date - interval '1' day and t1.order_id = t3.order_id
        where   1 = 1
          and   of.grass_date = current_date - interval '1' day
          and   t1.is_cb_shop = 0
          and   of.is_reverse = 0
          and   of.lm_shipment_company not in ('Standard Express')
        --group by 1, 2, 3
    )
    where   1 = 1
      --and   fulfilment_end_type = max_fulfilment_end_type
    group by 1
)


SELECT   distinct dntt.orderid
        , dntt.ordersn
        , dntt.consignment_no
        ,dntt.created_date
        ,dntt.status
        ,dntt.Shopee_Logistics_Status
        ,dntt.Order_status
        ,dntt.Payment_method
        ,dntt.shipping_traceno
        ,dntt.shipping_carrier
        ,dntt.payment_channel
        ,dntt.buyer_name
        ,dntt.buyer_userid
        ,dntt.seller_userid
        ,dntt.seller_user_name
        ,dntt.COD_amount
        ,dntt.escrow_to_seller
        ,Commission_Fee
        ,Seller_transaction_fee
        ,seller_voucher
        ,seller_shipping_rebate
        ,T1_buyer_shipping_fee
        ,T2_bank_card_rebate
        ,T3_seller_voucher_rebate
        ,T4_shopee_card_rebate
        ,T5_shopee_coin_rebate
        ,T6_shopee_voucher_rebate
        , coin_earned_shop
        ,ESF
        ,ASF
        ,(case
             when (COD_amount - T1_buyer_shipping_fee + T2_bank_card_rebate + T3_seller_voucher_rebate +T4_shopee_card_rebate +T5_shopee_coin_rebate+T6_shopee_voucher_rebate - cogs = 0 )
                 then 'True'
                 ELSE 'False'
             END) as Check_COGS
        ,service_fee
        , case when multi.nb_parcels IS NOT NULL then 'Multi WH' else 'Not Multi WH' end as Check_Multi_WH
        , pv_rebate_by_shopee_amt voucher_discount_shopee,
        insurance_premium_by_buyer_amt,
    buyer_paid_insurance_premium,
    premium,
    premium_after_discount
        , cogs
        ,cogs_item
        , gs.total_group check_group
        , re.refund_id
        , sum(cogs) total_cogs
    FROM DNTT
    left join multi on multi.order_id = dntt.orderid
    left join group_shipment gs
    on gs.lm_tracking_no = dntt.shipping_traceno
        left join marketplace.shopee_refund_v2_db__refund_v2_tab__vn_daily_s0_live re on re.order_id = DNTT.orderid
WHERE shipping_traceno IN ()
    -- Where shipping_traceno IN ${param_Input_LMTN}
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42
