\timing

BEGIN;

CREATE TEMPORARY TABLE tmp_buffered_event_pts_2
AS
  SELECT
      event_id,
      point_geom,
      ST_Buffer(point_geom, 0.025) AS buffered_pt
    FROM transcom_events
    WHERE (tmc IS NULL)
;

CREATE INDEX tmp_buffered_event_pts_2_idx ON tmp_buffered_event_pts_2 USING GIST (buffered_pt);
CLUSTER tmp_buffered_event_pts_2 USING tmp_buffered_event_pts_2_idx;
ANALYZE tmp_buffered_event_pts_2;

CREATE TEMPORARY TABLE tmp_buffered_tmcs_2
AS
  SELECT
      tmc,
      wkb_geometry,
      ST_Buffer(wkb_geometry, 0.025) AS buffered_tmc
    FROM ny.inrix_shapefile_20171107
;

CREATE INDEX tmp_buffered_tmcs_2_idx ON tmp_buffered_tmcs_2 USING GIST (buffered_tmc);
CLUSTER tmp_buffered_tmcs_2 USING tmp_buffered_tmcs_2_idx;
ANALYZE tmp_buffered_tmcs_2;

CREATE TABLE tmp_events_to_tmcs_2
AS
SELECT
    event_id,
    MIN(shp.tmc) AS tmc
  FROM tmp_buffered_event_pts_2 AS te
    INNER JOIN (
      SELECT
          event_id,
          MIN(ST_Distance(te.point_geom, shp.wkb_geometry))
            OVER (PARTITION BY event_id) AS min_dist
        FROM tmp_buffered_event_pts_2 AS te
          INNER JOIN tmp_buffered_tmcs_2 AS shp
          ON (te.buffered_pt && shp.buffered_tmc)
      ) AS sub_min_dists USING (event_id)
    INNER JOIN tmp_buffered_tmcs_2 AS shp
      ON (
        (te.buffered_pt && shp.buffered_tmc)
      )
    WHERE (ST_Distance(te.point_geom, shp.wkb_geometry) = sub_min_dists.min_dist)
    GROUP BY event_id
;

COMMIT;


-- ========== WITHOUT ANALYZE ==========
--                                                                                            QUERY PLAN                                                                                           
-- ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--  WindowAgg  (cost=1778667202.71..1781404685.33 rows=156427578 width=44)
--    ->  Sort  (cost=1778667202.71..1779058271.66 rows=156427578 width=44)
--          Sort Key: te.event_id
--          ->  Hash Join  (cost=504954060.68..1752564708.06 rows=156427578 width=44)
--                Hash Cond: (((te_1.event_id)::text = (te.event_id)::text) AND ((min(st_distance(te_1.point_geom, shp_1.wkb_geometry)) OVER (?)) = st_distance(te.point_geom, shp.wkb_geometry)))
--                ->  WindowAgg  (cost=193490.73..312131.29 rows=1483007 width=40)
--                      ->  Sort  (cost=193490.73..197198.25 rows=1483007 width=96)
--                            Sort Key: te_1.event_id
--                            ->  Nested Loop  (cost=0.28..41481.82 rows=1483007 width=96)
--                                  ->  Seq Scan on tmp_buffered_tmcs_2 shp_1  (cost=0.00..6201.96 rows=21096 width=64)
--                                  ->  Index Scan using tmp_buffered_event_pts_2_idx on tmp_buffered_event_pts_2 te_1  (cost=0.28..1.60 rows=7 width=96)
--                                        Index Cond: (buffered_pt && shp_1.buffered_tmc)
--                ->  Hash  (cost=143097292.15..143097292.15 rows=11444474520 width=108)
--                      ->  Nested Loop  (cost=0.00..143097292.15 rows=11444474520 width=108)
--                            ->  Seq Scan on transcom_events te  (cost=0.00..35105.95 rows=542495 width=44)
--                            ->  Materialize  (cost=0.00..6307.44 rows=21096 width=64)
--                                  ->  Seq Scan on tmp_buffered_tmcs_2 shp  (cost=0.00..6201.96 rows=21096 width=64)
-- (17 rows)
-- ========== WITH ANALYZE ==========
--
-- --------------------------------------------------------------------------------------------------------------------------------------------------------
--  WindowAgg  (cost=8890044.78..898823525.10 rows=57222373 width=44)
--    ->  Nested Loop  (cost=8890044.78..897965189.50 rows=57222373 width=22)
--          Join Filter: (st_distance(te.point_geom, shp.wkb_geometry) = (min(st_distance(te_1.point_geom, shp_1.wkb_geometry)) OVER (?)))
--          ->  Merge Join  (cost=8890044.78..11012186.50 rows=542495 width=52)
--                Merge Cond: ((te_1.event_id)::text = (te.event_id)::text)
--                ->  WindowAgg  (cost=8889840.85..10649704.29 rows=21998293 width=20)
--                      ->  Sort  (cost=8889840.85..8944836.58 rows=21998293 width=629)
--                            Sort Key: te_1.event_id
--                            ->  Nested Loop  (cost=0.28..41510.82 rows=21998293 width=629)
--                                  ->  Seq Scan on tmp_buffered_tmcs_2 shp_1  (cost=0.00..6174.96 rows=21096 width=2045)
--                                  ->  Index Scan using tmp_buffered_event_pts_2_idx on tmp_buffered_event_pts_2 te_1  (cost=0.28..1.61 rows=7 width=612)
--                                        Index Cond: (buffered_pt && shp_1.buffered_tmc)
--                ->  Index Scan using transcom_events_pkey on transcom_events te  (cost=0.42..80925.86 rows=542495 width=44)
--          ->  Materialize  (cost=0.00..6280.44 rows=21096 width=595)
--                ->  Seq Scan on tmp_buffered_tmcs_2 shp  (cost=0.00..6174.96 rows=21096 width=595)
-- (15 rows)

-- ========== REMOVED UNNECESSARY WINDOW FUNCTION, AVOID UNNECESSARY ST_Distance calls ==========
--                                                                        QUERY PLAN
-- --------------------------------------------------------------------------------------------------------------------------------------------------------
--  GroupAggregate  (cost=8902088.29..10996087.39 rows=70298 width=44)
--    Group Key: te.event_id
--    ->  Nested Loop  (cost=8902088.29..10994834.45 rows=109992 width=22)
--          Join Filter: (st_distance(te.point_geom, shp.wkb_geometry) = (min(st_distance(te_1.point_geom, shp_1.wkb_geometry)) OVER (?)))
--          ->  Merge Join  (cost=8902088.01..10937993.37 rows=70298 width=620)
--                Merge Cond: ((te_1.event_id)::text = (te.event_id)::text)
--                ->  WindowAgg  (cost=8889867.62..10649738.66 rows=21998388 width=20)
--                      ->  Sort  (cost=8889867.62..8944863.59 rows=21998388 width=629)
--                            Sort Key: te_1.event_id
--                            ->  Nested Loop  (cost=0.28..41500.82 rows=21998388 width=629)
--                                  ->  Seq Scan on tmp_buffered_tmcs_2 shp_1  (cost=0.00..6192.96 rows=21096 width=2045)
--                                  ->  Index Scan using tmp_buffered_event_pts_2_idx on tmp_buffered_event_pts_2 te_1  (cost=0.28..1.60 rows=7 width=612)
--                                        Index Cond: (buffered_pt && shp_1.buffered_tmc)
--                ->  Sort  (cost=12220.39..12396.13 rows=70298 width=612)
--                      Sort Key: te.event_id
--                      ->  Seq Scan on tmp_buffered_event_pts_2 te  (cost=0.00..6560.98 rows=70298 width=612)
--          ->  Index Scan using tmp_buffered_tmcs_2_idx on tmp_buffered_tmcs_2 shp  (cost=0.28..0.66 rows=2 width=2055)
--                Index Cond: (te.buffered_pt && buffered_tmc)
-- (18 rows)

-- 1781404685.33
--  898823525.10
--   10996087.39
