pivot {{ ref('mart__fact_trips_by_borough') }} on borough using first(trips);

select 
{{ dbt_utils.pivot(
    'borough',
    dbt_utils.get_column_values(ref('mart__fact_trips_by_borough'), 'borough'))}}
from {{ ref('mart__fact_trips_by_borough') }}