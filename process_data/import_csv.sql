
\echo "Первичный импорт данных из CSV (routes_raw)...";
drop table if exists routes_raw;
create table routes_raw (
    trip_id text,
    path jsonb,
    started_at timestamp,
    finished_at timestamp
);

\copy routes_raw from '/data/routes.csv' with (format csv, header true);
\echo "Готово";
