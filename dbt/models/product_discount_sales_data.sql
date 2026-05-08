{{ config(materialized='table') }}

with order_items as (
    select * from {{ source('raw', 'order_items') }}
),

products as (
    select * from {{ source('raw', 'products') }}
),

sold_products as (
    select
        product_sku,
        sum(quantity)                                                        as total_units_sold,
        sum(case when discount > 0 then quantity else 0 end)                 as units_sold_on_sale,
        round(avg(case when discount > 0 then discount else null end), 2)    as avg_discount,
        max(discount)                                                        as max_discount,
        max(unit_price)                                                      as unit_price
    from order_items
    group by product_sku
),

all_products as (
    select
        p.product_sku,
        p.product_name,
        p.unit_price,
        coalesce(s.total_units_sold,  0) as total_units_sold,
        coalesce(s.units_sold_on_sale, 0) as units_sold_on_sale,
        coalesce(s.avg_discount,       0) as avg_discount,
        coalesce(s.max_discount,       0) as max_discount,
        current_timestamp                 as info_date
    from products p
    left join sold_products s using (product_sku)
)

select * from all_products
