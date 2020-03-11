-----------------------------------------
-- Generate Hexagon Cells in Singapore
-- As of 15th Nov. 2019
-----------------------------------------

-- select version()
-- create extension postgis;

------------------------------------------
-- (1) UDF of generating hexagonal cells
-- source: https://gist.github.com/mjumbewu/1761802ea06fb78c596f9cf8c9b2e769#file-1-hexgrid_illustration-svg
------------------------------------------
DROP FUNCTION IF EXISTS generate_hexgrid(float, float, float, float, float, int);
CREATE OR REPLACE FUNCTION generate_hexgrid(width float, xmin float, ymin float, xmax float, ymax float, srid int default 3414)
RETURNS TABLE(
  gid text,
  geom geometry(Polygon)
) AS $grid$
declare
  b float := width / 2;
  a float := tan(radians(30)) * b;  -- tan(30) = 0.577350269
  c float := 2 * a;

  -- NOTE: The height of one cell is (2a + c), or about 1.154700538 * width.
  --       however, for each row, we shift vertically by (2[a + c]) to properly
  --       tesselate the hexagons. Thus, to determine the number of rows needed,
  --       we use the latter formula as the height of a row.
  height float := 2 * (a + c);

  -- Snap the min/max coords to a global grid according to the cell width, so
  -- that we minimize the chances of generating misaligned grids for overlapping
  -- regions.
  index_xmin int := floor(xmin / width);
  index_ymin int := floor(ymin / height);
  index_xmax int := ceil(xmax / width);
  index_ymax int := ceil(ymax / height);

  snap_xmin float := index_xmin * width;
  snap_ymin float := index_ymin * height;
  snap_xmax float := index_xmax * width;
  snap_ymax float := index_ymax * height;

  -- Calculate the total number of columns and rows. Note that the number of
  -- rows is actually half the number of rows, since each vertical iteration
  -- accounts for two "rows".
  ncol int := abs(index_xmax - index_xmin);
  nrow int := abs(index_ymax - index_ymin);

  polygon_string varchar := 'POLYGON((' ||
                                      0 || ' ' || 0         || ' , ' ||
                                      b || ' ' || a         || ' , ' ||
                                      b || ' ' || a + c     || ' , ' ||
                                      0 || ' ' || a + c + a || ' , ' ||
                                 -1 * b || ' ' || a + c     || ' , ' ||
                                 -1 * b || ' ' || a         || ' , ' ||
                                      0 || ' ' || 0         ||
                              '))';
BEGIN
  RETURN QUERY
  SELECT

    -- gid is made of the global x index of the cell, the global y index of the cell, and the cell width.
    format('%s %s %s',
           width,
           x_offset + (1 * x_series + index_xmin),
           y_offset + (2 * y_series + index_ymin)),

    -- geom is transformed using the width and height of a series, and set to
    -- the SRID specified.
    ST_SetSRID(ST_Translate(two_hex.geom,
                            x_series * width + snap_xmin,
                            y_series * height + snap_ymin), srid)

  FROM
    generate_series(0, ncol, 1) AS x_series,
    generate_series(0, nrow, 1) AS y_series,

    -- two_hex is a pair of hex cells, one roughly below the other. Thus, both
    -- have an x_offset of 0, but the second has a y_offset of 1.
    (
      -- Series cell #1
      SELECT
        0 AS x_offset,
        0 AS y_offset,
        polygon_string::geometry AS geom

      UNION
     
       -- Series cell #2
      SELECT
        0 AS x_offset,
        1 AS y_offset,
        ST_Translate(polygon_string::geometry, b , a + c)  AS geom
    ) AS two_hex;
END;
$grid$ LANGUAGE 'plpgsql';


---------------------------------------------------------------------------------
-- (2) Run the UDF of 'generate_hexgrid' to generate hexagon in Singapore
-- width=50, xmin=919.05, ymin=12576.34, xmax=54338.72, ymax=50172.05, srid=3414
---------------------------------------------------------------------------------
DROP TABLE IF EXISTS public.hex_gid;
CREATE TABLE public.hex_gid AS (
SELECT 
	ROW_NUMBER() OVER(ORDER BY b.x_series, b.y_series) AS hex_gid
	, b.*
FROM (
	SELECT 
	REPLACE(SPLIT_PART(a.hexgid, ',', 1), '(', '') AS hex_gid_combined
	, REPLACE(SPLIT_PART(REPLACE(SPLIT_PART(a.hexgid, ',', 1), '(', ''), ' ', 1), '"', '')::INT AS width
	, SPLIT_PART(REPLACE(SPLIT_PART(a.hexgid, ',', 1), '(', ''), ' ', 2)::INT AS x_series
	, REPLACE(SPLIT_PART(REPLACE(SPLIT_PART(a.hexgid, ',', 1), '(', ''), ' ', 3), '"', '')::INT AS y_series
	, REPLACE(SPLIT_PART(a.hexgid, ',', 2), ')', '') AS hex_poly
	FROM (
		SELECT 
		generate_hexgrid(50, 919.05, 12576.34, 54338.72, 50172.05, 3414)::text as hexgid
	) AS a
) b
);

--select * from public.hex_gid_tmp limit 10;


-------------------------------------------------------------
-- (3) convert hex_ploy format from text to geometry polygon
-------------------------------------------------------------
ALTER TABLE public.hex_gid
ALTER COLUMN hex_poly
TYPE GEOMETRY(POLYGON, 3414); -- SVY21 / Singapore TM, EPSG:3414
 
--select * from public.hex_gid_tmp limit 10;


----------------------------------------
-- (4) calculate 'hexagon center point'
----------------------------------------
DROP TABLE IF EXISTS public.hex_center;
CREATE TABLE public.hex_center AS (
SELECT 
	hex_gid
	, ST_Centroid(hex_poly) AS hex_center_point
FROM public.hex_gid
) 

--select * from public.hex_center limit 10;


---------------------------------------------------------------------
-- (5) find the nearest neighbors HEXAGON_GIDs from each hexagon_gid
---------------------------------------------------------------------
DROP TABLE IF EXISTS public.hex_neighbor;
CREATE TABLE public.hex_neighbor AS (
	WITH hex_center AS (
	SELECT *
	FROM public.hex_center
	), hex_center_tmp_b AS (
	SELECT 
		hex_gid AS hex_gid_b
		, hex_center_point AS hex_center_point_b
	FROM public.hex_center
	), hex_center_a_b AS (
	SELECT
		a.*
		, b.*
	FROM hex_center AS a, hex_center_tmp_b AS b
	), hex_center_dist AS (
	SELECT 
		hex_gid
		, hex_gid_b
		, ST_Distance(hex_center_point, hex_center_point_b) AS hex_center_dist
	FROM hex_center_a_b
	WHERE 
		ST_DWithin(hex_center_point, hex_center_point_b, 50) -- WIDTH 50
		AND ST_Distance(hex_center_point, hex_center_point_b) > 0 -- excluding it's own hex_gid
	ORDER BY hex_gid, hex_gid_b, hex_center_dist
	)
SELECT 
	hex_gid
	, ARRAY_AGG(hex_gid_b) AS hex_neighbor_gid
FROM hex_center_dist
GROUP BY hex_gid
);

--select * from public.hex_neighbor order by hex_gid limit 10;


-------------------------------------------------------------------
-- (6) make a hex_gid_master table by merging all tables by hex_gid
--    (a) coordinates
--    (b) hexagon centers
--    (c) nearest neighbors 
-------------------------------------------------------------------
DROP TABLE IF EXISTS public.hex_gid_master;
CREATE TABLE public.hex_gid_master AS (
SELECT 
	a.*
	, b.hex_center_point
	, c.hex_neighbor_gid
FROM public.hex_gid AS a 
INNER JOIN public.hex_center AS b ON a.hex_gid = b.hex_gid
INNER JOIN public.hex_neighbor AS c ON a.hex_gid = c.hex_gid
);

--SELECT * FROM public.hex_gid_master LIMIT 100;


-------------------------------
-- (7) create geospatial index
-------------------------------
CREATE INDEX idx_geom ON public.hex_gid_master USING GIST(hex_poly);


-----------------------
-- (8) drop temp table
-----------------------
DROP TABLE IF EXISTS public.hex_gid;
DROP TABLE IF EXISTS public.hex_center;
DROP TABLE IF EXISTS public.hex_neighbor;


----------------------------------
-- (9) check hex_gid_master table
----------------------------------
/*
SELECT COUNT(*) FROM public.hex_gid_master; -- 234,768
SELECT * FROM public.hex_gid_master ORDER BY hex_gid LIMIT 1000;

SELECT MIN(X_SERIES) FROM public.hex_gid_master; -- 9
SELECT MAX(X_SERIES) FROM public.hex_gid_master; -- 544

SELECT MIN(Y_SERIES) FROM public.hex_gid_master; -- 72
SELECT MAX(Y_SERIES) FROM public.hex_gid_master; -- 509
*/
