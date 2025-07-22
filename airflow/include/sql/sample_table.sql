select *

from {{ params.database }}.{{ params.schema }}.{{ params.table_name }} sample (5)

limit 50