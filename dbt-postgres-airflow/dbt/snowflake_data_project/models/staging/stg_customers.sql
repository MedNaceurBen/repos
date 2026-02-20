select 
    id as id_customer,
    name as name_customer,
    email,
    country
from 
    {{ source ('raw_data', 'customers')}}




