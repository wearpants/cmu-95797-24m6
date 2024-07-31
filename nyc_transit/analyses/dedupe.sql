with events as (
    select
        strptime(insert_timestamp, '%d/%m/%Y %H:%M') as insert_timestamp,
        event_id,
        event_type,
        user_id,
        strptime(event_timestamp, '%d/%m/%Y %H:%M') as event_timestamp
    from {{ ref('events') }}
)
select
    *
from events
qualify row_number() over (partition by event_id order by insert_timestamp desc) = 1

--what would have gone wrong had you used rank?