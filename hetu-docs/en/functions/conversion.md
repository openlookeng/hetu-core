
Conversion Functions
====================

openLooKeng will implicitly convert numeric and character values to the correct type if such a conversion is possible. For any other types, by default, openLooKeng will not convert it implicitly. For example, a query that expects a
varchar will not automatically convert a bigint value to an equivalent varchar.

When necessary, values can be explicitly cast to a particular type.

Or, you can enable the `implicit conversion` functionality, then openLooKeng will try to auto apply conversion between source type and target type.

Conversion Functions
--------------------

**cast(value AS type)** -\> type

Explicitly cast a value as a type. This can be used to cast a varchar to a numeric value type and vice versa.

**try\_cast(value AS type)** -\> type

Like `cast`, but returns null if the cast fails.


Formatting
----------

**format(format, args\...)** -\> varchar

Returns a formatted string using the specified [format string](https://docs.oracle.com/javase/8/docs/api/java/util/Formatter.html#syntax) and arguments:

    SELECT format('%s%%', 123); -- '123%'
    SELECT format('%.5f', pi()); -- '3.14159'
    SELECT format('%03d', 8); -- '008'
    SELECT format('%,.2f', 1234567.89); -- '1,234,567.89'
    SELECT format('%-7s,%7s', 'hello', 'world'); --  'hello  ,  world'
    SELECT format('%2$s %3$s %1$s', 'a', 'b', 'c'); -- 'b c a'
    SELECT format('%1$tA, %1$tB %1$te, %1$tY', date '2006-07-04'); -- 'Tuesday, July 4, 2006'


Data Size
---------

The `parse_presto_data_size` function supports the following units:

| Unit | Description | Value  |
| :--- | :---------- | :----- |
| `B`  | Bytes       | 1      |
| `kB` | Kilobytes   | 1024   |
| `MB` | Megabytes   | 1024^2 |
| `GB` | Gigabytes   | 1024^3 |
| `TB` | Terabytes   | 1024^4 |
| `PB` | Petabytes   | 1024^5 |
| `EB` | Exabytes    | 1024^6 |
| `ZB` | Zettabytes  | 1024^7 |
| `YB` | Yottabytes  | 1024^8 |

**parse\_presto\_data\_size(string)** -\> decimal(38)

Parses `string` of format `value unit` into a number, where `value` is the fractional number of `unit` values:

    SELECT parse_presto_data_size('1B'); -- 1
    SELECT parse_presto_data_size('1kB'); -- 1024
    SELECT parse_presto_data_size('1MB'); -- 1048576
    SELECT parse_presto_data_size('2.3MB'); -- 2411724


Implicit Conversion
-------------------

You can enable the implicit conversion vis setting session property:

    SET SESSION implicit_conversion=true

By default, the property value is false, the implicit conversion is turned off.

If set it to true, whenever openLooKeng found the type is not match, but probably can be compatible with each other, openLooKeng will automatically rewrite the statement by applying the `CAST` function.

In this way, user do not need to explicitly add `CAST` function to convert it.

For example, a query that expects a varchar will automatically convert a bigint value to an equivalent varchar.

Obviously, not all data types are compatible with each other, below table lists all the feasible conversion for all basic datatype.

|           | BOOLEAN | TINYINT | SMALLINT | INTEGER | BIGINT | REAL | DOUBLE | DECIMAL | VARCHAR | CHAR | VARBINARY | JSON | DATE  | TIME  | TIME WITH TIME ZONE | TIMESTAMP | TIMESTAMP WITH TIME ZONE |
| --------- | ------- | ------- | -------- | ------- | ------ | ---- | ------ | ------- | ------- | ---- | --------- | ---- | ----- | ----- | ------------------- | --------- | ------------------------ |
| BOOLEAN   |         | Y(1)    | Y        | Y       | Y      | Y    | Y      | Y       | Y(2)    | N    | N         | Y    | N     | N     | N                   | N         | N                        |
| TINYINT   | Y(3)    |         | Y        | Y       | Y      | Y    | Y      | Y       | Y       | N    | N         | Y    | N     | N     | N                   | N         | N                        |
| SMALLINT  | Y       | Y(4)    |          | Y       | Y      | Y    | Y      | Y       | Y       | N    | N         | Y    | N     | N     | N                   | N         | N                        |
| INTEGER   | Y       | Y       | Y        |         | Y      | Y    | Y      | Y       | Y       | N    | N         | Y    | N     | N     | N                   | N         | N                        |
| BIGINT    | Y       | Y       | Y        | Y       |        | Y    | Y      | Y       | Y       | N    | N         | Y    | N     | N     | N                   | N         | N                        |
| REAL      | Y       | Y       | Y        | Y       | Y      |      | Y      | Y(5)    | Y       | N    | N         | Y    | N     | N     | N                   | N         | N                        |
| DOUBLE    | Y       | Y       | Y        | Y       | Y      | Y    |        | Y       | Y       | N    | N         | Y    | N     | N     | N                   | N         | N                        |
| DECIMAL   | Y       | Y       | Y        | Y       | Y      | Y    | Y      | (6)     | Y       | N    | N         | Y    | N     | N     | N                   | N         | N                        |
| VARCHAR   | Y(7)    | Y       | Y        | Y       | Y      | Y    | Y      | Y(8)    |         | Y(9) | Y         | Y    | Y(10) | Y(11) | Y(12)               | Y(13)     | Y                        |
| CHAR      | N       | N       | N        | N       | N      | N    | N      | N       | Y       |      | N         | N    | N     | N     | N                   | N         | N                        |
| VARBINARY | N       | N       | N        | N       | N      | N    | N      | N       | N       | N    |           | N    | N     | N     | N                   | N         | N                        |
| JSON      | N       | N       | N        | N       | N      | N    | N      | N       | Y       | N    | N         |      | N     | N     | N                   | N         | N                        |
| DATE      | N       | N       | N        | N       | N      | N    | N      | N       | Y       | N    | N         | Y    |       | N     | N                   | Y(14)     | Y                        |
| TIME      | N       | N       | N        | N       | N      | N    | N      | N       | Y       | N    | N         | N    | N     |       | Y(15)               | Y(16)     | Y                        |
| TIME WITH | N       | N       | N        | N       | N      | N    | N      | N       | Y       | N    | N         | N    | N     | Y     |                     | Y         | Y                        |
| TIME ZONE |         |         |          |         |        |      |        |         |         |      |           |      |       |       |                     |           |                          |
| TIMESTAMP | N       | N       | N        | N       | N      | N    | N      | N       | Y       | N    | N         | N    | Y     | Y     | Y                   |           | Y                        |
| TIMESTAMP | N       | N       | N        | N       | N      | N    | N      | N       | Y       | N    | N         | N    | Y     | Y     | Y                   | Y         |                          |
| WITH TIME |         |         |          |         |        |      |        |         |         |      |           |      |       |       |                     |           |                          |
| ZONE      |         |         |          |         |        |      |        |         |         |      |           |      |       |       |                     |           |                          |

**Note:**

- Y or Y(#): standard for support implicit convert. But there might be some limitation need your attention. Please refer to below item.
- N: standard for not support implicit convert

(1): BOOLEAN-\>NUMBER the converted result can be only 0 or 1

(2): BOOLEAN-\>VARCHAR the converted result can be only TRUE or FALSE

(3): NUMBER -\> BOOLEAN 0 will be converted to false, others will be converted to true

(4): BIG PRECISION -\> SAMLL conversion will fail when data is out of range of SMALL

(5): REAL/FLOAT -\>DECIMAL conversion will fail when data is out of range of DECIMAL. Scale will be cut off when out of range.

(6): DECIMAL-\>DECIMAL conversion will fail when data is out of range of DECIMAL. Scale will be cut off when out of range.

(7): VARCHAR-\>BOOLEAN only \'0\',\'1\',\'TRUE\',\'FALSE\' can be converted. Others will be failed

(8): VARCHAR-\>DECIMAL conversion will fail when it's not an numeric or the converted value is out of range of DECIMAL. Scale will be cut off when out of range.

(9): VARCHAR-\>CHAR if length of VARCHAR is larger than CHAR, it will be cut off.

(10): VARCHAR-\>DATE The VARCHAR can only be formatted like: \'YYYY-MM-DD\', e.g. 2000-01-01

(11): VARCHAR-\>TIME The VARCHAR can only be formatted like: \'HH:MM:SS.XXX\'

(12): VARCHAR-\>TIME ZONE The VARCHAR can only be formatted like: \'HH:MM:SS.XXX XXX\', e.g. 01:02:03.456 America/Los\_Angeles

(13): VARCHAR-\>TIMESTAMP The VARCHAR can only be formatted like: YYYY-MM-DD HH:MM:SS.XXX

(14): DATE-\>TIMESTAMP will auto padding the time with 0. e.g. \'2010-01-01\' -> 2010-01-01 00:00:00.000

(15): TIME-\>TIME WITH TIME ZONE will auto padding the default time zone

(16): TIME-\>TIMESTAMP will auto add the default date: 1970-01-01

Miscellaneous
-------------

**typeof(expr)** -\> varchar

Returns the name of the type of the provided expression:

    SELECT typeof(123); -- integer
    SELECT typeof('cat'); -- varchar(3)
    SELECT typeof(cos(2) + 1.5); -- double

