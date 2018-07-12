## ST_Envelope_Aggr

Introduction: Return the entire envelope boundary of all geometries in A

Format: `ST_Envelope_Aggr (A:geometryColumn)`

Since: `v1.0.0`

Spark SQL example:
```SQL
SELECT ST_Envelope_Aggr(pointdf.arealandmark)
FROM pointdf
```

## ST_Union_Aggr

Introduction: Return the polygon union of all polygons in A

Format: `ST_Union_Aggr (A:geometryColumn)`

Since: `v1.0.0`

Spark SQL example:
```SQL
SELECT ST_Union_Aggr(polygondf.polygonshape)
FROM polygondf
```