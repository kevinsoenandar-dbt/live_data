create table if not exists {{ params.database }}.{{ params.schema }}.{{ params.table_name }} (
    {{ params.definitions }}
);