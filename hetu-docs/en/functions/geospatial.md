
Geospatial Functions
====================

openLooKeng Geospatial functions that begin with the `ST_` prefix support the SQL/MM specification and are compliant with the Open Geospatial Consortium's (OGC) OpenGIS Specifications. As such, many openLooKeng Geospatial functions require, or more accurately, assume that geometries that are operated on are both simple and valid. For example, it does not make sense to calculate the area of a polygon that has a hole defined outside of the polygon, or to construct a polygon from a non-simple boundary
line.

openLooKeng Geospatial functions support the Well-Known Text (WKT) and Well-Known Binary (WKB) form of spatial objects:

-   `POINT (0 0)`
-   `LINESTRING (0 0, 1 1, 1 2)`
-   `POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))`
-   `MULTIPOINT (0 0, 1 2)`
-   `MULTILINESTRING ((0 0, 1 1, 1 2), (2 3, 3 2, 5 4))`
-   `MULTIPOLYGON (((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1)), ((-1 -1, -1 -2, -2 -2, -2 -1, -1 -1)))`
-   `GEOMETRYCOLLECTION (POINT(2 3), LINESTRING (2 3, 3 4))`

Use `ST_GeometryFromText` and `ST_GeomFromBinary` functions to create geometry objects from WKT or WKB.

The `SphericalGeography` type provides native support for spatial features represented on *geographic* coordinates (sometimes called *geodetic* coordinates, or *lat/lon*, or *lon/lat*). Geographic coordinates are spherical coordinates expressed in angular units (degrees).

The basis for the `Geometry` type is a plane. The shortest path between two points on the plane is a straight line. That means calculations on geometries (areas, distances, lengths, intersections, etc) can be calculated using cartesian mathematics and straight line vectors.

The basis for the `SphericalGeography` type is a sphere. The shortest path between two points on the sphere is a great circle arc. That means that calculations on geographies (areas, distances, lengths, intersections, etc) must be calculated on the sphere, using more complicated mathematics. More accurate measurements that take the actual spheroidal shape of the world into account are not supported.

Values returned by the measurement functions `ST_Distance` and `ST_Length` are in the unit of meters; values returned by `ST_Area` are in square meters.

Use `to_spherical_geography()`  function to convert a geometry object to geography object.

For example, `ST_Distance(ST_Point(-71.0882, 42.3607), ST_Point(-74.1197, 40.6976))`
returns `3.4577` in the unit of the passed-in values on the euclidean plane, while
`ST_Distance(to_spherical_geography(ST_Point(-71.0882, 42.3607)), to_spherical_geography(ST_Point(-74.1197, 40.6976)))` returns `312822.179` in meters.

Constructors
------------

**ST\_AsBinary(Geometry)** -\> varbinary

Returns the WKB representation of the geometry.


**ST\_AsText(Geometry)** -\> varchar

Returns the WKT representation of the geometry. For empty geometries, `ST_AsText(ST_LineFromText('LINESTRING EMPTY'))` produces `'MULTILINESTRING EMPTY'` and `ST_AsText(ST_Polygon('POLYGON EMPTY'))` produces `'MULTIPOLYGON EMPTY'`.

**ST\_GeometryFromText(varchar)** -\> Geometry

Returns a geometry type object from WKT representation.

**ST\_GeomFromBinary(varbinary)** -\> Geometry

Returns a geometry type object from WKB representation.

**ST\_LineFromText(varchar)** -\> LineString

Returns a geometry type linestring object from WKT representation.

**ST\_LineString(array(Point))** -\> LineString

Returns a LineString formed from an array of points. If there are fewer than two non-empty points in the input array, an empty LineString will be returned. Throws an exception if any element in the array is `NULL`
or empty or same as the previous one. The returned geometry may not be simple, e.g. may self-intersect or may contain duplicate vertexes depending on the input.

**ST\_MultiPoint(array(Point))** -\> MultiPoint

Returns a MultiPoint geometry object formed from the specified points.
Return `NULL` if input array is empty. Throws an exception if any element in the array is `NULL` or empty. The returned geometry may not be simple and may contain duplicate points if input array has duplicates.

**ST\_Point(double, double)** -\> Point

Returns a geometry type point object with the given coordinate values.

**ST\_Polygon(varchar)** -\> Polygon

Returns a geometry type polygon object from WKT representation.

**to\_spherical\_geography(Geometry)** -\> SphericalGeography

Converts a Geometry object to a SphericalGeography object on the sphere of the Earth\'s radius. This function is only applicable to `POINT`, `MULTIPOINT`, `LINESTRING`, `MULTILINESTRING`, `POLYGON`, `MULTIPOLYGON` geometries defined in 2D space, or `GEOMETRYCOLLECTION` of such geometries. For each point of the input geometry, it verifies that `point.x` is within `[-180.0, 180.0]` and `point.y` is within
`[-90.0, 90.0]`, and uses them as (longitude, latitude) degrees to construct the shape of the `SphericalGeography` result.

**to\_geometry(SphericalGeography)** -\> Geometry

Converts a SphericalGeography object to a Geometry object.


Relationship Tests
------------------

**ST\_Contains(Geometry, Geometry)** -\> boolean

Returns `true` if and only if no points of the second geometry lie in the exterior of the first geometry, and at least one point of the interior of the first geometry lies in the interior of the second geometry.

**ST\_Crosses(Geometry, Geometry)** -\> boolean

Returns `true` if the supplied geometries have some, but not all, interior points in common.

**ST\_Disjoint(Geometry, Geometry)** -\> boolean

Returns `true` if the give geometries do not *spatially intersect* \--if they do not share any space together.

**ST\_Equals(Geometry, Geometry)** -\> boolean

Returns `true` if the given geometries represent the same geometry. 


**ST\_Intersects(Geometry, Geometry)** -\> boolean

Returns `true` if the given geometries spatially intersect in two
dimensions (share any portion of space) and `false` if they do not (they
are disjoint).


**ST\_Overlaps(Geometry, Geometry)** -\> boolean

Returns `true` if the given geometries share space, are of the same dimension, but are not completely contained by each other.

**ST\_Relate(Geometry, Geometry)** -\> boolean

Returns `true` if first geometry is spatially related to second geometry.


**ST\_Touches(Geometry, Geometry)** -\> boolean

Returns `true` if the given geometries have at least one point in common, but their interiors do not intersect.


**ST\_Within(Geometry, Geometry)** -\> boolean

Returns `true` if first geometry is completely inside second geometry.


Operations
----------

**geometry\_union(array(Geometry))** -\> Geometry

Returns a geometry that represents the point set union of the input geometries. Performance of this function, in conjunction with `array_agg` to first aggregate the input geometries, may be better than `geometry_union_agg`, at the expense of higher memory utilization.

**ST\_Boundary(Geometry)** -\> Geometry

Returns the closure of the combinatorial boundary of this geometry.


**ST\_Buffer(Geometry, distance)** -\> Geometry

Returns the geometry that represents all points whose distance from the specified geometry is less than or equal to the specified distance.

**ST\_Difference(Geometry, Geometry)** -\> Geometry

Returns the geometry value that represents the point set difference of the given geometries.

**ST\_Envelope(Geometry)** -\> Geometry

Returns the bounding rectangular polygon of a geometry.

**ST\_EnvelopeAsPts(Geometry)** -\> array(Geometry)

Returns an array of two points: the lower left and upper right corners of the bounding rectangular polygon of a geometry. Returns null if input geometry is empty.

**ST\_ExteriorRing(Geometry)** -\> Geometry

Returns a line string representing the exterior ring of the input polygon.

**ST\_Intersection(Geometry, Geometry)** -\> Geometry

Returns the geometry value that represents the point set intersection of two geometries.

**ST\_SymDifference(Geometry, Geometry)** -\> Geometry

Returns the geometry value that represents the point set symmetric difference of two geometries.

**ST\_Union(Geometry, Geometry)** -\> Geometry

Returns a geometry that represents the point set union of the input geometries.

See also: `geometry_union`, `geometry_union_agg`


Accessors
---------

**ST\_Area(Geometry)** -\> double

Returns the 2D Euclidean area of a geometry.

For Point and LineString types, returns 0.0. For GeometryCollection types, returns the sum of the areas of the individual geometries.

**ST\_Area(SphericalGeography)** -\> double

Returns the area of a polygon or multi-polygon in square meters using a spherical model for Earth.

**ST\_Centroid(Geometry)** -\> Geometry

Returns the point value that is the mathematical centroid of a geometry.

**ST\_ConvexHull(Geometry)** -\> Geometry

Returns the minimum convex geometry that encloses all input geometries.

**ST\_CoordDim(Geometry)** -\> bigint

Return the coordinate dimension of the geometry.


**ST\_Dimension(Geometry)** -\> bigint

Returns the inherent dimension of this geometry object, which must be less than or equal to the coordinate dimension.


**ST\_Distance(Geometry, Geometry)** -\> double

Returns the 2-dimensional cartesian minimum distance (based on spatial ref) between two geometries in projected units.


**ST\_Distance(SphericalGeography, SphericalGeography)** -\> double

Returns the great-circle distance in meters between two SphericalGeography points.


**ST\_GeometryN(Geometry, index)** -\> Geometry

Returns the geometry element at a given index (indices start at 1). If the geometry is a collection of geometries (for example, GEOMETRYCOLLECTION or MULTI\*), returns the geometry at a given index. If the given index is less than 1 or greater than the total number of elements in the collection, returns `NULL`. Use
`` `ST_NumGeometries ` to find out the total number of elements. Singular geometries (for example, POINT, LINESTRING, POLYGON), are treated as collections of one element. Empty geometries are treated as empty collections.

**ST\_InteriorRingN(Geometry, index)** -\> Geometry

Returns the interior ring element at the specified index (indices start at 1). If the given index is less than 1 or greater than the total number of interior rings in the input geometry, returns `NULL`. Throws an error if the input geometry is not a polygon. Use  `ST_NumInteriorRing ` to find out the total number of elements.

**ST\_GeometryType(Geometry)** -\> varchar

Returns the type of the geometry.


**ST\_IsClosed(Geometry)** -\> boolean

Returns `true` if the linestring\'s start and end points are coincident.


**ST\_IsEmpty(Geometry)** -\> boolean

Returns `true` if this Geometry is an empty geometrycollection, polygon, point etc.

**ST\_IsSimple(Geometry)** -\> boolean

Returns `true` if this Geometry has no anomalous geometric points, such as self intersection or self tangency.

**ST\_IsRing(Geometry)** -\> boolean

Returns `true` if and only if the line is closed and simple.

**ST\_IsValid(Geometry)** -\> boolean

Returns `true` if and only if the input geometry is well formed. Use `geometry_invalid_reason` to determine why the geometry is not well formed.

**ST\_Length(Geometry)** -\> double

Returns the length of a linestring or multi-linestring using Euclidean measurement on a two dimensional plane (based on spatial ref) in projected units.

**ST\_PointN(LineString, index)** -\> Point

Returns the vertex of a linestring at a given index (indices start at 1). If the given index is less than 1 or greater than the total number of elements in the collection, returns `NULL`. Use `` ST_NumPoints `` to find out the total number of elements.

**ST\_Points(Geometry)** -\> array(Point)

Returns an array of points in a linestring.


**ST\_XMax(Geometry)** -\> double

Returns X maxima of a bounding box of a geometry.


**ST\_YMax(Geometry)** -\> double

Returns Y maxima of a bounding box of a geometry.


**ST\_XMin(Geometry)** -\> double

Returns X minima of a bounding box of a geometry.


**ST\_YMin(Geometry)** -\> double

Returns Y minima of a bounding box of a geometry.


**ST\_StartPoint(Geometry)** -\> point

Returns the first point of a LineString geometry as a Point. This is a shortcut for ST\_PointN(geometry, 1).

**simplify\_geometry(Geometry, double)** -\> Geometry

Returns a \"simplified\" version of the input geometry using the Douglas-Peucker algorithm. Will avoid creating derived geometries (polygons in particular) that are invalid.

**ST\_EndPoint(Geometry)** -\> point

Returns the last point of a LineString geometry as a Point. This is a shortcut for ST\_PointN(geometry, ST\_NumPoints(geometry)).

**ST\_X(Point)** -\> double

Return the X coordinate of the point.

**ST\_Y(Point)** -\> double

Return the Y coordinate of the point.

**ST\_InteriorRings(Geometry)** -\> Geometry

Returns an array of all interior rings found in the input geometry, or an empty array if the polygon has no interior rings. Returns null if the input geometry is empty. Throws an error if the input geometry is not a
polygon.


**ST\_NumGeometries(Geometry)** -\> bigint

Returns the number of geometries in the collection. If the geometry is a collection of geometries (for example, GEOMETRYCOLLECTION or MULTI\*), returns the number of geometries, for single geometries returns 1, for empty geometries returns 0.


**ST\_Geometries(Geometry)** -\> Geometry

Returns an array of geometries in the specified collection. Returns a one-element array if the input geometry is not a multi-geometry. Returns null if input geometry is empty.

**ST\_NumPoints(Geometry)** -\> bigint

Returns the number of points in a geometry. This is an extension to the SQL/MM `ST_NumPoints` function which only applies to point and linestring.


**ST\_NumInteriorRing(Geometry)** -\> bigint

Returns the cardinality of the collection of interior rings of a polygon.

**line\_locate\_point(LineString, Point)** -\> double

Returns a float between 0 and 1 representing the location of the closest point on the LineString to the given Point, as a fraction of total 2d line length.

Returns `NULL` if a LineString or a Point is empty or `NULL`.

**geometry\_invalid\_reason(Geometry)** -\> varchar

Returns the reason for why the input geometry is not valid. Returns null if the input is valid.

**great\_circle\_distance(latitude1, longitude1, latitude2, longitude2)**-\> double

Returns the great-circle distance between two points on Earth\'s surface in kilometers.


Aggregations
------------

**convex\_hull\_agg(Geometry)** -\> Geometry

Returns the minimum convex geometry that encloses all input geometries.


**geometry\_union\_agg(Geometry)** -\> Geometry

Returns a geometry that represents the point set union of all input geometries.


Bing Tiles
----------

These functions convert between geometries and [Bing tiles](https://msdn.microsoft.com/en-us/library/bb259689.aspx).

**bing\_tile(x, y, zoom\_level)** -\> BingTile

Creates a Bing tile object from XY coordinates and a zoom level. Zoom levels from 1 to 23 are supported.

**bing\_tile(quadKey)** -\> BingTile

Creates a Bing tile object from a quadkey.

**bing\_tile\_at(latitude, longitude, zoom\_level)** -\> BingTile

Returns a Bing tile at a given zoom level containing a point at a given latitude and longitude. Latitude must be within `[-85.05112878, 85.05112878]` range. Longitude must be within `[-180, 180]` range. Zoom levels from 1 to 23 are supported.

**bing\_tiles\_around(latitude, longitude, zoom\_level)** -\> array(BingTile)

Returns a collection of Bing tiles that surround the point specified by the latitude and longitude arguments at a given zoom level.

**bing\_tiles\_around(latitude, longitude, zoom\_level, radius\_in\_km)** -\> array(BingTile)

Returns a minimum set of Bing tiles at specified zoom level that cover a circle of specified radius in km around a specified (latitude, longitude) point.


**bing\_tile\_coordinates(tile)** -\> row\<x, y\>

Returns the XY coordinates of a given Bing tile.


**bing\_tile\_polygon(tile)** -\> Geometry

Returns the polygon representation of a given Bing tile.


**bing\_tile\_quadkey(tile)** -\> varchar

Returns the quadkey of a given Bing tile.


**bing\_tile\_zoom\_level(tile)** -\> tinyint

Returns the zoom level of a given Bing tile.


**geometry\_to\_bing\_tiles(geometry, zoom\_level)** -\> array(BingTile)

Returns the minimum set of Bing tiles that fully covers a given geometry at a given zoom level. Zoom levels from 1 to 23 are supported.

