


\echo "Собираем геометрию маршрутов (routes)...";
drop table if exists routes; 
create table routes as 
select trip_id, started_at, finished_at,
	st_setsrid(
	    st_makeline(
	        st_point(
	            (latlon ->> 'lng')::float,
	            (latlon ->> 'lat')::float
	        )
	        order by idx
	    ),
	    4326
	)::geometry(linestring, 4326) geom
from (
    select r.*, idx, latlon
    from routes_raw r, json_array_elements(path::json -> 'coordinates') with ordinality as coord(latlon, idx)
    where latlon::text !~* '\y0\.0\y' -- отфильтровываю 0.0 координаты как заведомый мусор
) coords
group by trip_id, started_at, finished_at;

create index on routes using gist(geom);
\echo "Готово";












\echo "Раскладываем треки на сегменты, считаем среднюю скорость на каждый сегмент (route_segments)...";
-- (исходя из предположения что все точки трека должны передаваться через равные промежутки времени)
drop table if exists route_segments;
create table route_segments as 
--
select 
	r.trip_id, 
	g.path[1] sid, 
	round(st_length(g.geom::geography)::numeric, 1) segment_length,
	round(round(st_length(g.geom::geography)::numeric, 1) / 
		((extract(epoch from r.finished_at) - extract(epoch from r.started_at)) / 
			(st_npoints(r.geom) - 1) / 3.6
		), 1
	) segment_speed_kmh, 
	g.geom
from routes 			    r, 
	 st_dumpsegments(geom) 	g;

create index on route_segments using gist(geom);
create index on route_segments (segment_speed_kmh);
\echo "Готово";



\echo "Пересобираю маршруты выкидывая отрезки длиннее 30 м. (routes_clean)...";
drop table if exists routes_clean;
create table routes_clean as
select trip_id, started_at, finished_at, 
	(extract(hour from started_at))::int2 "hour",
	sum(segment_length) length_filtered, 
	(st_collect(geom))::geometry(geometry, 4326) geom
from route_segments 			s 
left join routes_raw 	r using(trip_id)
where segment_length  < 30
group by trip_id, started_at, finished_at;

create index on routes_clean using gist(geom);
\echo "Готово";




-- Упрощённая геометрия треков для отображения на кврте:
drop table if exists routes_simplified;
create table routes_simplified as 
--
select r.trip_id, r.hour, r.started_at, r.finished_at, 
	st_simplify(st_linemerge(r.geom), 0.00002)::geometry(geometry, 4326) geom
from routes_clean r;

create index on routes_simplified using gist(geom);






\echo "Разбивка дорог на 20м сегменты (road_segments)...";
drop table if exists road_segments;
create table road_segments as
--
with road_lengths as (
    select 
        id,
        city,
        highway,
        name,
        geom,
        st_length(geom::geography) as length_meters
    from roads
),
road_segments as (
    select 
        id,
        city,
        highway,
        name,
        n,
        st_linesubstring(
            geom,
            (n * 20) / length_meters,
            least(((n + 1) * 20) / length_meters, 1.0)
        ) as segment_geom
    from road_lengths
    cross join generate_series(0, floor(length_meters / 20)::int) as n
    where length_meters > 20
) 
select 
    id,
    n as segment_id,
    city,
    highway,
    name,
    segment_geom::geometry(geometry, 4326) geom
from road_segments
where segment_geom is not null
union all 
select 
    id,
    1 segment_id,
    city,
    highway,
    name,
    geom::geometry(geometry, 4326) geom
from road_lengths
where length_meters < 20;

create index on road_segments using gist(geom);
create index on road_segments (city);
\echo "Готово";




\echo "Анализ дорог: считаю число поездок и среднюю скорость на сегментах в 20м. (roads_stat)...";
drop table if exists roads_stat;
create table roads_stat as
--
select r.segment_id, r.geom::geometry(geometry, 4326) geom,
	count(distinct tr.trip_id)::int2 													    trip_count,
	round(coalesce(percentile_cont(0.5) within group(order by segment_speed_kmh), 0))::int2 med_speed   -- медианная скорость на участке
from road_segments r
left join route_segments tr 
    on st_dwithin(r.geom, tr.geom, 0.0002) -- ~20 метров в wgs84
        and segment_length < 30
group by r.segment_id, r.geom;

create index on roads_stat using gist(geom);
create index on roads_stat(trip_count);
create index on roads_stat(med_speed);
\echo "Готово";





-- Кластеризуем начальные и конечные точки маршрута,
-- чтобы выявить точки с наибольшим спросом
drop table if exists clusters;
create table clusters as 
with start_points as (
    select 
        trip_id,
        st_startpoint(st_geometryn(geom, 1)) as geom,
        'start' as point_type
    from routes_clean
    where not st_isempty(geom)
),
end_points as (
    select 
        trip_id,
        st_endpoint(st_geometryn(geom, st_numgeometries(geom))) as geom,
        'end' as point_type
    from routes_clean
    where not st_isempty(geom)
),
all_points as (
    -- объединяем стартовые и конечные точки
    select * from start_points
    union all
    select * from end_points
),
clusters as (
    -- кластеризуем все точки (DBSCAN с параметрами: 50 м и минимум 5 точек в кластере)
    select 
        trip_id,
        geom,
        point_type,
        st_clusterdbscan(geom, eps := 0.00025, minpoints := 5) over () as cluster_id
    from all_points
)
-- финализируем: для каждого кластера считаем количество точек и тип (start/end)
select 
    cluster_id,
    point_type,
    count(distinct trip_id) as trip_count,
    st_centroid(st_collect(geom))::geometry(point, 4326) as geom  -- центроид кластера
from clusters
where cluster_id is not null  -- исключаем шумовые точки (без кластера)
group by cluster_id, point_type
having count(distinct trip_id) >= 5
order by trip_count desc;

create index on clusters using gist(geom);





-- Пробовал создать function source чтобы на лету отдавать разную геометрию в зависимости от масштаба, 
-- но он у меня не завёлся. Упёрся в то что martin не инджектит json из комментария в tilejson. Убил много времени и бросил. 

-- drop table if exists trip_stat_detail;
-- create table trip_stat_detail as 
-- --
-- with clustered as (
--     select
--         st_clusterintersectingwin(geom) over (partition by trip_count) as cluster_id,
--         trip_count,
--         geom
--     from public.roads_stat
-- )
-- select
--     cluster_id,
--     trip_count,
--     st_simplify(st_union(geom), 5)::geometry(geometry, 4326) as geom
-- from clustered
-- group by cluster_id, trip_count;

-- create index on trip_stat_detail using gist(geom);
 



-- drop table if exists trip_stat_medium;
-- create table trip_stat_medium as 
-- --
-- with agg as (
-- 	select geom, 
-- 		case 
-- 			when trip_count = 0   then 0 
-- 			when trip_count < 10  then 10 
-- 			when trip_count < 25  then 25
-- 			when trip_count < 50  then 50 
-- 			when trip_count < 200 then 200 
-- 			else 250
-- 		end trip_count
-- 	from roads_stat
-- ),
-- clustered as (
--     select
--         st_clusterintersectingwin(geom) over (partition by trip_count) as cluster_id,
--         trip_count,
--         geom
--     from agg
-- )
-- select
--     cluster_id,
--     trip_count,
--     st_simplify(st_union(geom), 5)::geometry(geometry, 4326) as geom  -- упрощение + объединение
-- from clustered
-- group by cluster_id, trip_count;
-- create index on trip_stat_medium using gist(geom);




-- drop table if exists trip_stat_low;
-- create table trip_stat_low as
-- --
-- with agg as (
-- 	select geom, 
-- 		case 
-- 			when trip_count = 0   then 0 
-- 			when trip_count < 10  then 10 
-- 			when trip_count < 25  then 25
-- 			when trip_count < 50  then 50 
-- 			when trip_count < 200 then 200 
-- 			else 250
-- 		end trip_count
-- 	from roads_stat
-- ),
-- clustered as (
--     select
--         st_clusterintersectingwin(geom) over (partition by trip_count) as cluster_id,
--         trip_count,
--         geom
--     from agg
-- )
-- select
--     cluster_id,
--     trip_count,
--     st_simplify(st_union(geom), 8)::geometry(geometry, 4326) as geom  -- упрощение + объединение
-- from clustered
-- group by cluster_id, trip_count
-- having round(st_length(st_simplify(st_union(geom), 8)::geography)) > 100;

-- create index on trip_stat_low using gist(geom);


-- create or replace function get_trip_stat_by_zoom(z integer, x integer, y integer)
-- returns table(geom geometry, trips numeric, avg_speed numeric) as $$
-- begin
--     if z >= 12 then
--         return query
--         select r.geom geom, r.trip_count
--         from trip_stat_detail r
--         where r.geom && st_tileenvelope(z, x, y);
--     elsif z between 9 and 11 then
--         return query
--         select r.geom, r.trip_count
--         from trip_stat_medium r
--         where r.geom && st_tileenvelope(z, x, y);
--     else
--         return query
--         select r.geom, r.trip_count
--         from trip_stat_low r
--         where r.geom && st_tileenvelope(z, x, y);
--     end if;
-- end;
-- $$ language plpgsql;


-- drop function if exists get_trip_stat_by_zoom;
-- create or replace function get_trip_stat_by_zoom(z integer, x integer, y integer)
-- returns bytea as $$
-- declare mvt bytea;
-- begin
--     if z >= 12 then
-- 	  	select into mvt st_asmvt(tile, 'get_trip_stat_by_zoom_query', 4096, 'geom') from (
-- 	    	select cluster_id, trip_count,
-- 	      		st_asmvtgeom(
-- 	          		st_transform(st_curvetoline(geom), 3857),
-- 	          		st_tileenvelope(z, x, y),
-- 	          		4096, 64, true
-- 				) as geom
-- 	    	from trip_stat_detail
-- 	    	where geom && st_transform(st_tileenvelope(z, x, y), 4326)
-- 	 	) as tile where geom is not null;
--     elsif z between 9 and 11 then
-- 	  	select into mvt st_asmvt(tile, 'get_trip_stat_by_zoom_query', 4096, 'geom') from (
-- 	    	select cluster_id, trip_count,
-- 	      		st_asmvtgeom(
-- 	          		st_transform(st_curvetoline(geom), 3857),
-- 	          		st_tileenvelope(z, x, y),
-- 	          		4096, 64, true
-- 				) as geom
-- 	    	from trip_stat_medium
-- 	    	where geom && st_transform(st_tileenvelope(z, x, y), 4326)
-- 	 	) as tile where geom is not null;
--     else
-- 	  	select into mvt st_asmvt(tile, 'get_trip_stat_by_zoom_query', 4096, 'geom') from (
-- 	    	select cluster_id, trip_count,
-- 	      		st_asmvtgeom(
-- 	          		st_transform(st_curvetoline(geom), 3857),
-- 	          		st_tileenvelope(z, x, y),
-- 	          		4096, 64, true
-- 				) as geom
-- 	    	from trip_stat_medium
-- 	    	where geom && st_transform(st_tileenvelope(z, x, y), 4326)
-- 	 	) as tile where geom is not null;	
-- 	end if;
-- return mvt;
-- end
-- $$ language plpgsql immutable strict parallel safe;



-- DO $do$ BEGIN
--     EXECUTE 'COMMENT ON FUNCTION get_trip_stat_by_zoom IS $tj$' || $$
--     {
--         "description": "my new description",
--         "attribution": "my attribution",
--         "name": "trip_count",
--         "vector_layers": [
--             {
--                 "id": "trip_stat",
--                 "fields": {
--                     "cluster_id": "Number",
--                     "trip_count": "Number"
--                 }
--             }
--         ]
--     }
--     $$::json || '$tj$';
-- END $do$;
