
# 地理空间函数

以`ST_`前缀开头的openLooKeng 地理空间函数支持SQL/MM规范，并符合开放地理空间联盟(OGC)的OpenGIS规范。因此，许多openLooKeng地理空间函数要求（更准确地说是假定）所操作的几何图形是简单且有效的。例如，计算在外部定义了一个洞的多边形的面积或者通过非简单边界线构造一个多边形是没有意义的。

openLooKeng地理空间函数支持熟知文本(WKT)和熟知二进制(WKB)形式的空间对象：

- `POINT (0 0)`
- `LINESTRING (0 0, 1 1, 1 2)`
- `POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))`
- `MULTIPOINT (0 0, 1 2)`
- `MULTILINESTRING ((0 0, 1 1, 1 2), (2 3, 3 2, 5 4))`
- `MULTIPOLYGON (((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1)), ((-1 -1, -1 -2, -2 -2, -2 -1, -1 -1)))`
- `GEOMETRYCOLLECTION (POINT(2 3), LINESTRING (2 3, 3 4))`

使用`ST_GeometryFromText`和`ST_GeomFromBinary`函数可以从WKT或WKB创建几何对象。

`SphericalGeography`类型提供对地理坐标（有时称为*大地*坐标、*lat/lon*或*lon/lat*）上表示的空间特征的原生支持。地理坐标是以角度单位（角度）表示的球面坐标。

`Geometry`类型的基础是平面。平面上两点之间的最短路径是一条直线。这意味着可以使用笛卡尔数学知识和直线向量来计算几何计算（面积、距离、长度、交点等）。

`SphericalGeography`类型的基础是球面。球面上两点之间的最短路径是一个大圆弧。这意味着必须使用更复杂的数学知识在球面上进行地理计算（面积、距离、长度、交点等）计算。不支持需要考虑世界实际呈球形的精确测量。

测量函数`ST_Distance`和`ST_Length`的返回值以米为单位；`ST_Area`的返回值以平方米为单位。

使用`to_spherical_geography()`函数可以将几何对象转换为地理对象。

例如，在欧几里得平面上`ST_Distance(ST_Point(-71.0882, 42.3607), ST_Point(-74.1197, 40.6976))`返回`3.4577`并且该值与传入的值具有相同的单位，而`ST_Distance(to_spherical_geography(ST_Point(-71.0882, 42.3607)), to_spherical_geography(ST_Point(-74.1197, 40.6976)))`返回`312822.179`，该值以米为单位。

## 构造函数

**ST\_AsBinary(Geometry)** -> varbinary

返回几何图像的WKB表示形式。

**ST\_AsText(Geometry)** -> varchar

返回几何图像的WKT表示形式。对于空的几何图形，`ST_AsText(ST_LineFromText('LINESTRING EMPTY'))`将生成`'MULTILINESTRING EMPTY'`，`ST_AsText(ST_Polygon('POLYGON EMPTY'))`将生成`'MULTIPOLYGON EMPTY'`。

**ST\_GeometryFromText(varchar)** -> Geometry

从WKT表示形式返回一个几何类型对象。

**ST\_GeomFromBinary(varbinary)** -> Geometry

从WKB表示形式返回一个几何类型对象。

**ST\_LineFromText(varchar)** -> LineString

从WKT表示形式返回一个几何类型LineString对象。

**ST\_LineString(array(Point))** -> LineString

返回由一个点数组形成的LineString。如果输入数组中的非空点少于两个，则返回空LineString。如果该数组中的任何元素为`NULL`、空或与前一个元素相同，则抛出异常。返回的几何图形可能不是简单的几何图形，例如，可能自相交或可能包含重复的顶点，具体取决于输入。

**ST\_MultiPoint(array(Point))** -> MultiPoint

返回通过指定的点形成的MultiPoint几何对象。如果输入数组为空，则返回`NULL`。如果数组中的任何元素为`NULL`或空，则抛出异常。如果输入数组有重复项，则返回的几何图形可能不是简单图形，可能包含重复的点。

**ST\_Point(double, double)** -> Point

返回具有给定坐标值的几何类型Point对象。

**ST\_Polygon(varchar)** -> Polygon

从WKT表示形式返回一个几何类型Polygon对象。

**to\_spherical\_geography(Geometry)** -> SphericalGeography

将Geometry对象转换为地球半径球面上的SphericalGeography对象。该函数仅适用于二维空间中定义的`POINT`、`MULTIPOINT`、`LINESTRING`、`MULTILINESTRING`、`POLYGON`和`MULTIPOLYGON`几何图形或此类几何图形的`GEOMETRYCOLLECTION`。对于输入几何形状的每个点，该函数验证`point.x`是否处于`[-180.0, 180.0]`范围之内以及`point.y`是否处于`[-90.0, 90.0]`范围之内，并且将其作为（经度、纬度）度数来构造`SphericalGeography`结果的形状。

**to\_geometry(SphericalGeography)** -> Geometry

将SphericalGeography对象转换为Geometry对象。

## 关系测试

**ST\_Contains(Geometry, Geometry)** -> boolean

当且仅当第二个几何图形的任何点都不位于第一个几何图形的外部，并且第一个几何图形的内部至少有一个点位于第二个几何图形的内部时返回`true`。

**ST\_Crosses(Geometry, Geometry)** -> boolean

如果提供的几何图形具有一些（但不是全部）共同的内部点，则返回`true`。

**ST\_Disjoint(Geometry, Geometry)** -> boolean

如果给定的几何图形未*在空间中相交*（即未占据相同的空间），则返回`true`。

**ST\_Equals(Geometry, Geometry)** -> boolean

如果给定的几何图形表示相同的几何图形，则返回`true`。

**ST\_Intersects(Geometry, Geometry)** -> boolean

如果给定的几何图形在二维空间中相交（占据任何相同的空间），则返回`true`；如果未相交，则返回`false`。

**ST\_Overlaps(Geometry, Geometry)** -> boolean

如果给定的几何图形占据相同的空间并具有相同的尺寸，但彼此之间不完全包含，则返回`true`。

**ST\_Relate(Geometry, Geometry)** -> boolean

如果第一个几何图形与第二个几何图形在空间上相关，则返回`true`。

**ST\_Touches(Geometry, Geometry)** -> boolean

如果给定的几何图形至少有一个共同点，但其内部不相交，则返回`true`。

**ST\_Within(Geometry, Geometry)** -> boolean

如果第一个几何图形完全位于第二个几何图形的内部，则返回`true`。

## 运算

**geometry\_union(array(Geometry))** -> Geometry

返回表示输入几何图形的点集并集的几何图形。该函数与`array_agg`相结合对输入几何图形进行首次聚集的性能可能要好于`geometry_union_agg`，但需要消耗更多的内存。

**ST\_Boundary(Geometry)** -> Geometry

返回该几何图形的组合边界的闭包。

**ST\_Buffer(Geometry, distance)** -> Geometry

返回表示与指定几何图形的距离小于等于指定距离的所有点的几何图形。

**ST\_Difference(Geometry, Geometry)** -> Geometry

返回表示给定几何图形的点集差集的几何图形值。

**ST\_Envelope(Geometry)** -> Geometry

返回几何图形的外接矩形。

**ST\_EnvelopeAsPts(Geometry)** -> array(Geometry)

返回一个包含两个点的数组：几何图形的外接矩形的左下角和右上角。如果输入几何图形为空，则返回NULL。

**ST\_ExteriorRing(Geometry)** -> Geometry

返回表示输入多边形外环的折线。

**ST\_Intersection(Geometry, Geometry)** -> Geometry

返回表示两个几何图形的点集交集的几何图形值。

**ST\_SymDifference(Geometry, Geometry)** -> Geometry

返回表示两个几何图形的点集对称差集的几何图形值。

**ST\_Union(Geometry, Geometry)** -> Geometry

返回表示输入几何图形的点集并集的几何图形。

另请参见：`geometry_union`、`geometry_union_agg`

## 访问器

**ST\_Area(Geometry)** -> double

返回几何图形的二维欧几里得面积。

对于Point和LineString类型，返回0.0。对于GeometryCollection类型，返回各个几何图形的面积之和。

**ST\_Area(SphericalGeography)** -> double

返回一个多边形或多多边形的面积（以平方米为单位，使用地球的球面模型）。

**ST\_Centroid(Geometry)** -> Geometry

返回作为几何图形的数学质心的点值。

**ST\_ConvexHull(Geometry)** -> Geometry

返回包含所有输入几何图形的最小凸几何图形。

**ST\_CoordDim(Geometry)** -> bigint

返回几何图形的坐标维度。

**ST\_Dimension(Geometry)** -> bigint

返回该几何对象的固有维度，该维度必须小于等于坐标维度。

**ST\_Distance(Geometry, Geometry)** -> double

返回两个几何图形之间的二维笛卡尔最小距离（基于空间参考），以投影单元为单位。

**ST\_Distance(SphericalGeography, SphericalGeography)** -> double

返回两个SphericalGeography点之间的大圆距离（以米为单位）。

**ST\_GeometryN(Geometry, index)** -> Geometry

返回给定索引处的几何元素（索引从1开始）。如果几何图形是几何图形的集合（例如GEOMETRYCOLLECTION或MULTI\*），则返回给定索引处的几何图形。如果给定的索引小于1或大于集合中的元素总数，则返回`NULL`。使用`ST_NumGeometries`可以求出元素的总数。奇异几何图形（例如POINT、LINESTRING、POLYGON）被视为包含一个元素的集合。空几何图形被视为空集合。

**ST\_InteriorRingN(Geometry, index)** -> Geometry

返回指定索引处的内环元素（索引从1开始）。如果给定的索引小于1或大于输入几何图形中内环的总数，则返回`NULL`。如果输入几何图形不是多边形，则抛出错误。使用 `ST_NumInteriorRing`可以求出元素的总数。

**ST\_GeometryType(Geometry)** -> varchar

返回几何图形的类型。

**ST\_IsClosed(Geometry)** -> boolean

如果折线的起点和终点重合，则返回`true`。

**ST\_IsEmpty(Geometry)** -> boolean

如果该Geometry是一个空GeometryCollection、Polygon、Point等，则返回`true`。

**ST\_IsSimple(Geometry)** -> boolean

如果该Geometry没有异常的几何点（如自相交或自切），则返回`true`。

**ST\_IsRing(Geometry)** -> boolean

当且仅当线是闭合且简单的时返回`true`。

**ST\_IsValid(Geometry)** -> boolean

当且仅当输入几何图形形状规则时返回`true`。使用`geometry_invalid_reason`可以确定几何图形形状不规则的原因。

**ST\_Length(Geometry)** -> double

使用二维平面上的欧几里得测量返回单个折线或多折线的长度（基于空间参考），仪投影单元为单位。

**ST\_PointN(LineString, index)** -> Point

返回在给定索引处（索引从1开始）的折线顶点。如果给定的索引小于1或大于集合中的元素总数，则返回`NULL`。使用`ST_NumPoints`可以求出元素的总数。

**ST\_Points(Geometry)** -> array(Point)

返回包含一条折线中的点的数组。

**ST\_XMax(Geometry)** -> double

返回几何图形边界框的X坐标最大值。

**ST\_YMax(Geometry)** -> double

返回几何图形边界框的Y坐标最大值。

**ST\_XMin(Geometry)** -> double

返回几何图形边界框的X坐标最小值。

**ST\_YMin(Geometry)** -> double

返回几何图形边界框的Y坐标最小值。

**ST\_StartPoint(Geometry)** -> point

以Point形式返回LineString几何图形的第一个点。这是ST\_PointN(geometry, 1)的简化形式。

**simplify\_geometry(Geometry, double)** -> Geometry

使用Douglas-Peucker算法返回输入几何图形的“简化”版本。将避免创建无效的推导几何图形（尤其是多边形）。

**ST\_EndPoint(Geometry)** -> point

以Point形式返回LineString几何图形的最后一个点。这是ST\_PointN(geometry, ST\_NumPoints(geometry))的简化形式。

**ST\_X(Point)** -> double

返回点的X坐标。

**ST\_Y(Point)** -> double

返回点的Y坐标。

**ST\_InteriorRings(Geometry)** -> Geometry

返回包含输入几何图形中的所有内环的数组，如果多边形没有内环，则返回空数组。如果输入几何图形为空，则返回NULL。如果输入几何图形不是多边形，则抛出错误。

**ST\_NumGeometries(Geometry)** -> bigint

返回集合中几何图形的数量。如果几何图形是几何图形集合（例如GEOMETRYCOLLECTION或MULTI\*），则返回几何图形的数量：对于单个几何图形，返回1；对于空几何图形，返回0。

**ST\_Geometries(Geometry)** -> Geometry

返回包含指定集合中的几何图形的数组。如果输入几何图形不是多几何图形，则返回一个单元素数组。如果输入几何图形为空，则返回NULL。

**ST\_NumPoints(Geometry)** -> bigint

返回几何图形中点的数量。这是对SQL/MM **`ST_NumPoints`**函数的扩展，该函数仅适用于点和折线。

**ST\_NumInteriorRing(Geometry)** -> bigint

返回多边形内环集合的基数。

**line\_locate\_point(LineString, Point)** -> double

返回一个介于0和1之间的浮点数，该浮点数表示LineString上距离给定Point最近的点的位置（作为总二维线长的一部分）。

如果LineString或Point为空或`NULL`，则返回`NULL`。

**geometry\_invalid\_reason(Geometry)** -> varchar

返回输入几何图形无效的原因。如果输入有效，则返回NULL。

**great\_circle\_distance(latitude1, longitude1, latitude2, longitude2)**-> double

返回地球表面两点间以千米为单位的大圆距离。

## 聚合

**convex\_hull\_agg(Geometry)** -> Geometry

返回包含所有输入几何图形的最小凸几何图形。

**geometry\_union\_agg(Geometry)** -> Geometry

返回表示所有输入几何图形的点集并集的几何图形。

## Bing图块

这些函数使几何图形和[Bing图块](https://msdn.microsoft.com/en-us/library/bb259689.aspx)相互转换。

**bing\_tile(x, y, zoom\_level)** -> BingTile

从XY坐标和缩放级别创建Bing图块对象。支持的缩放级别为1至23。

**bing\_tile(quadKey)** -> BingTile

从四叉树键创建Bing图块对象。

**bing\_tile\_at(latitude, longitude, zoom\_level)** -> BingTile

返回一个具有给定缩放级别的Bing图块，其中包含一个具有给定纬度和经度的点。纬度必须处于`[-85.05112878, 85.05112878]`范围之内。经度必须处于`[-180, 180]`范围之内。支持的缩放级别为1至23。

**bing\_tiles\_around(latitude, longitude, zoom\_level)** -> array(BingTile)

返回围绕由纬度和经度参数指定的点且具有给定缩放级别的Bing图块集合。

**bing\_tiles\_around(latitude, longitude, zoom\_level, radius\_in\_km)** -> array(BingTile)

返回一个在指定（纬度、经度）点周围覆盖具有指定半径（以km为单位）的圆的Bing图块（具有指定的缩放级别）最小集合。

**bing\_tile\_coordinates(tile)** -> row\<x, y>

返回给定Bing图块的XY坐标。

**bing\_tile\_polygon(tile)** -> Geometry

返回给定Bing图块的多边形表示形式。

**bing\_tile\_quadkey(tile)** -> varchar

返回给定Bing图块的四叉树键。

**bing\_tile\_zoom\_level(tile)** -> tinyint

返回给定Bing图块的缩放级别。

**geometry\_to\_bing\_tiles(geometry, zoom\_level)** -> array(BingTile)

返回在给定缩放级别完全覆盖给定几何图形的Bing图块最小集合。支持的缩放级别为1至23。