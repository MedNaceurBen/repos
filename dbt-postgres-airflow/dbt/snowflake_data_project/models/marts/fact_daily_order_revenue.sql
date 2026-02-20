select 
    O.order_date,
    O.order_id,
    sum(OI.total_price) as total_price
from 
    {{ ref('stg_orders') }} as O 
left join 
    {{ ref('stg_order_items') }} as OI
    on O.order_id = OI.order_id
group by 
    O.order_date, 
    O.order_id
order by 
    O.order_date
