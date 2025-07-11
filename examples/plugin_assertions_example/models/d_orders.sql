with orders as (

    select *
    from (
        values
            (1, 'CREDIT_CARD', 100, 10),
            (2, 'DEBIT', 0, -5),
            (3, null, 30, null)
    ) as t(id, payment_method, amount, tax)

)

select
    *,
    {{ dbt_assertions.assertions() }}
from orders
