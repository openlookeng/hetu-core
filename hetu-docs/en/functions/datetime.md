
Date and Time Functions and Operators
=====================================

Date and Time Operators
-----------------------

| Operator | Example                                             | Result                    |
| :------- | :-------------------------------------------------- | :------------------------ |
| `+`      | `date '2012-08-08' + interval '2' day`              | `2012-08-10`              |
| `+`      | `time '01:00' + interval '3' hour`                  | `04:00:00.000`            |
| `+`      | `timestamp '2012-08-08 01:00' + interval '29' hour` | `2012-08-09 06:00:00.000` |
| `+`      | `timestamp '2012-10-31 01:00' + interval '1' month` | `2012-11-30 01:00:00.000` |
| `+`      | `interval '2' day + interval '3' hour`              | `2 03:00:00.000`          |
| `+`      | `interval '3' year + interval '5' month`            | `3-5`                     |
| `-`      | `date '2012-08-08' - interval '2' day`              | `2012-08-06`              |
| `-`      | `time '01:00' - interval '3' hour`                  | `22:00:00.000`            |
| `-`      | `timestamp '2012-08-08 01:00' - interval '29' hour` | `2012-08-06 20:00:00.000` |
| `-`      | `timestamp '2012-10-31 01:00' - interval '1' month` | `2012-09-30 01:00:00.000` |
| `-`      | `interval '2' day - interval '3' hour`              | `1 21:00:00.000`          |
| `-`      | `interval '3' year - interval '5' month`            | `2-7`                     |

Time Zone Conversion
--------------------

The `AT TIME ZONE` operator sets the time zone of a timestamp:

    SELECT timestamp '2012-10-31 01:00 UTC';
    2012-10-31 01:00:00.000 UTC
    
    SELECT timestamp '2012-10-31 01:00 UTC' AT TIME ZONE 'America/Los_Angeles';
    2012-10-30 18:00:00.000 America/Los_Angeles

Date and Time Functions
-----------------------

**current\_date** -\> date

Returns the current date as of the start of the query.


**current\_time** -\> time with time zone

Returns the current time as of the start of the query.


**current\_timestamp** -\> timestamp with time zone

Returns the current timestamp as of the start of the query.


**current\_timezone()** -\> varchar

Returns the current time zone in the format defined by IANA (e.g., `America/Los_Angeles`) or as fixed offset from UTC (e.g., `+08:35`)


**date(x)** -\> date

This is an alias for `CAST(x AS date)`.


**from\_iso8601\_timestamp(string)** -\> timestamp with time zone

Parses the ISO 8601 formatted `string` into a `timestamp with time zone`.


**from\_iso8601\_date(string)** -\> date

Parses the ISO 8601 formatted `string` into a `date`.


**from\_unixtime(unixtime)** -\> timestamp

Returns the UNIX timestamp `unixtime` as a timestamp. `unixtime` is the number of seconds since `1970-01-01 00:00:00`.

**from\_unixtime(unixtime, string)** -\> timestamp with time zone

Returns the UNIX timestamp `unixtime` as a timestamp with time zone using `string` for the time zone. `unixtime` is the number of seconds since `1970-01-01 00:00:00`.

**from\_unixtime(unixtime, hours, minutes)** -\> timestamp with time zone

Returns the UNIX timestamp `unixtime` as a timestamp with time zone using `hours` and `minutes` for the time zone offset. `unixtime` is the number of seconds since `1970-01-01 00:00:00`.

**localtime** -\> time

Returns the current time as of the start of the query.

**localtimestamp** -\> timestamp

Returns the current timestamp as of the start of the query.

**now()** -\> timestamp with time zone

This is an alias for `current_timestamp`.


**to\_iso8601(x)** -\> varchar

Formats `x` as an ISO 8601 string. `x` can be date, timestamp, or timestamp with time zone.


**to\_milliseconds(interval)** -\> bigint

Returns the day-to-second `interval` as milliseconds.


**to\_unixtime(timestamp)** -\> double

Returns `timestamp` as a UNIX timestamp.


**Note**

The following SQL-standard functions do not use parenthesis:

-   `current_date`
-   `current_time`
-   `current_timestamp`
-   `localtime`
-   `localtimestamp`


Truncation Function
-------------------

The `date_trunc` function supports the following units:

| Unit      | Example Truncated Value   |
| :-------- | :------------------------ |
| `second`  | `2001-08-22 03:04:05.000` |
| `minute`  | `2001-08-22 03:04:00.000` |
| `hour`    | `2001-08-22 03:00:00.000` |
| `day`     | `2001-08-22 00:00:00.000` |
| `week`    | `2001-08-20 00:00:00.000` |
| `month`   | `2001-08-01 00:00:00.000` |
| `quarter` | `2001-07-01 00:00:00.000` |
| `year`    | `2001-01-01 00:00:00.000` |

The above examples use the timestamp `2001-08-22 03:04:05.321` as the
input.

**date\_trunc(unit, x)** -\> \[same as input\]

Returns `x` truncated to `unit`.


Interval Functions
------------------

The functions in this section support the following interval units:

 

| Unit          | Description        |
| :------------ | :----------------- |
| `millisecond` | Milliseconds       |
| `second`      | Seconds            |
| `minute`      | Minutes            |
| `hour`        | Hours              |
| `day`         | Days               |
| `week`        | Weeks              |
| `month`       | Months             |
| `quarter`     | Quarters of a year |
| `year`        | Years              |

**date\_add(unit, value, timestamp)** -\> \[same as input\]

Adds an interval `value` of type `unit` to `timestamp`. Subtraction can be performed by using a negative value.


**date\_diff(unit, timestamp1, timestamp2)** -\> bigint

Returns `timestamp2 - timestamp1` expressed in terms of `unit`.


Duration Function
-----------------

The `parse_duration` function supports the following units:

 

| Unit | Description  |
| :--- | :----------- |
| `ns` | Nanoseconds  |
| `us` | Microseconds |
| `ms` | Milliseconds |
| `s`  | Seconds      |
| `m`  | Minutes      |
| `h`  | Hours        |
| `d`  | Days         |

**parse\_duration(string)** -\> interval

Parses `string` of format `value unit` into an interval, where `value` is fractional number of `unit` values:

    SELECT parse_duration('42.8ms'); -- 0 00:00:00.043
    SELECT parse_duration('3.81 d'); -- 3 19:26:24.000
    SELECT parse_duration('5m');     -- 0 00:05:00.000


MySQL Date Functions
--------------------

The functions in this section use a format string that is compatible with the MySQL `date_parse` and `str_to_date` functions. The following table, based on the MySQL manual, describes the format specifiers:

| Specifier | Description                                                  |
| --------- | ------------------------------------------------------------ |
| `%a`      | Abbreviated weekday name (`Sun` .. `Sat`)                    |
| `%b`      | Abbreviated month name (`Jan` .. `Dec`)                      |
| `%c`      | Month, numeric (`1` .. `12`) [4]                      |
| `%D`      | Day of the month with English suffix (`0th`, `1st`, `2nd`, `3rd`, …) |
| `%d`      | Day of the month, numeric (`01` .. `31`) [4]          |
| `%e`      | Day of the month, numeric (`1` .. `31`) [4]           |
| `%f`      | Fraction of second (6 digits for printing: `000000` .. `999000`; 1 - 9 digits for parsing: `0` .. `999999999`) [1] |
| `%H`      | Hour (`00` .. `23`)                                          |
| `%h`      | Hour (`01` .. `12`)                                          |
| `%I`      | Hour (`01` .. `12`)                                          |
| `%i`      | Minutes, numeric (`00` .. `59`)                              |
| `%j`      | Day of year (`001` .. `366`)                                 |
| `%k`      | Hour (`0` .. `23`)                                           |
| `%l`      | Hour (`1` .. `12`)                                           |
| `%M`      | Month name (`January` .. `December`)                         |
| `%m`      | Month, numeric (`01` .. `12`) [4]                     |
| `%p`      | `AM` or `PM`                                                 |
| `%r`      | Time, 12-hour (`hh:mm:ss` followed by `AM` or `PM`)          |
| `%S`      | Seconds (`00` .. `59`)                                       |
| `%s`      | Seconds (`00` .. `59`)                                       |
| `%T`      | Time, 24-hour (`hh:mm:ss`)                                   |
| `%U`      | Week (`00` .. `53`), where Sunday is the first day of the week |
| `%u`      | Week (`00` .. `53`), where Monday is the first day of the week |
| `%V`      | Week (`01` .. `53`), where Sunday is the first day of the week; used with `%X` |
| `%v`      | Week (`01` .. `53`), where Monday is the first day of the week; used with `%x` |
| `%W`      | Weekday name (`Sunday` .. `Saturday`)                        |
| `%w`      | Day of the week (`0` .. `6`), where Sunday is the first day of the week [3] |
| `%X`      | Year for the week where Sunday is the first day of the week, numeric, four digits; used with `%V` |
| `%x`      | Year for the week, where Monday is the first day of the week, numeric, four digits; used with `%v` |
| `%Y`      | Year, numeric, four digits                                   |
| `%y`      | Year, numeric (two digits) [2]                       |
| `%%`      | A literal `%` character                                      |
| `%x`      | `x`, for any `x` not listed above                            |

| [1] | Timestamp is truncated to milliseconds. |
| ------------ | --------------------------------------- |
|              |                                         |

| [2] | When parsing, two-digit year format assumes range `1970` .. `2069`, so “70” will result in year `1970` but “69” will produce `2069`. |
| ------------ | ------------------------------------------------------------ |
|              |                                                              |

| [3] | This specifier is not supported yet. Consider using [`day_of_week()`](#day-of-week) (it uses `1-7` instead of `0-6`). |
| ------------ | ------------------------------------------------------------ |
|              |                                                              |

| [4] | *([1], [2], [3], [4])* This specifier does not support `0` as a month or day. |
| ---- | ------------------------------------------------------------ |
|      |                                                              |

**Warning**

The following specifiers are not currently supported:
`%D %U %u %V %w %X`


**date\_format(timestamp, format)** -\> varchar

Formats `timestamp` as a string using `format`.


**date\_parse(string, format)** -\> timestamp

Parses `string` into a timestamp using `format`.


Java Date Functions
-------------------

The functions in this section use a format string that is compatible with JodaTime\'s
[DateTimeFormat](http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html) pattern format.

**format\_datetime(timestamp, format)** -\> varchar

Formats `timestamp` as a string using `format`.

**parse\_datetime(string, format)** -\> timestamp with time zone

Parses `string` into a timestamp with time zone using `format`.


Extraction Function
-------------------

The `extract` function supports the following fields: 

| Field             | Description                                                  |
| :---------------- | :----------------------------------------------------------- |
| `YEAR`            | [`year()`](#year) |
| `QUARTER`         | [`quarter()`](#quarter) |
| `MONTH`           | [`month()`](#month) |
| `WEEK`            | [`week()`](#week) |
| `DAY`             | [`day()`](#day) |
| `DAY_OF_MONTH`    | [`day()`](#day) |
| `DAY_OF_WEEK`     | [`day_of_week()`](#day-of-week) |
| `DOW`             | [`day_of_week()`](#day-of-week) |
| `DAY_OF_YEAR`     | [`day_of_year()`](#day-of-year) |
| `DOY`             | [`day_of_year()`](#day-of-year) |
| `YEAR_OF_WEEK`    | [`year_of_week()`](#year-of-week) |
| `YOW`             | [`year_of_week()`](#year-of-week) |
| `HOUR`            | [`hour()`](#hour) |
| `MINUTE`          | [`minute()`](#minute) |
| `SECOND`          | [`second()`](#second) |
| `TIMEZONE_HOUR`   | [`timezone_hour()`](#timezone-hour) |
| `TIMEZONE_MINUTE` | [`timezone_minute()`](#timezone-minute) |

The types supported by the `extract` function vary depending on the field to be extracted. Most fields support all date and time types.

**extract(field FROM x)** -\> bigint

Returns `field` from `x`.

**Note**

This SQL-standard function uses special syntax for specifying the arguments.

   

Convenience Extraction Functions
--------------------------------
#### DAY
**day(x)** -\> bigint

Returns the day of the month from `x`.

#### DAY OF MONTH
**day\_of\_month(x)** -\> bigint

This is an alias for `day`.

#### DAY OF WEEK
**day\_of\_week(x)** -\> bigint

Returns the ISO day of the week from `x`. The value ranges from `1` (Monday) to `7` (Sunday).

#### DAY OF YEAR
**day\_of\_year(x)** -\> bigint

Returns the day of the year from `x`. The value ranges from `1` to `366`.

#### DOW
**dow(x)** -\> bigint

This is an alias for `day_of_week`.

#### DOY
**doy(x)** -\> bigint

This is an alias for `day_of_year`.

#### HOUR
**hour(x)** -\> bigint

Returns the hour of the day from `x`. The value ranges from `0` to `23`.

#### MILLISECOND
**millisecond(x)** -\> bigint

Returns the millisecond of the second from `x`.

#### MINUTE
**minute(x)** -\> bigint

Returns the minute of the hour from `x`.

#### MONTH
**month(x)** -\> bigint

Returns the month of the year from `x`.

#### QUARTER
**quarter(x)** -\> bigint

Returns the quarter of the year from `x`. The value ranges from `1` to `4`.

#### SECOND
**second(x)** -\> bigint

Returns the second of the minute from `x`.

#### TIMEZONE HOURS
**timezone\_hour(timestamp)** -\> bigint

Returns the hour of the time zone offset from `timestamp`.

#### TIMEZONE MINUTE
**timezone\_minute(timestamp)** -\> bigint

Returns the minute of the time zone offset from `timestamp`.

#### WEEK
**week(x)** -\> bigint

Returns the [ISO week](https://calendars.wikia.org/wiki/ISO_week_date) of the year from `x`. The value ranges from `1`
to `53`.

#### WEEK OF YEAR
**week\_of\_year(x)** -\> bigint

This is an alias for `week`.

#### YEAR
**year(x)** -\> bigint

Returns the year from `x`.

#### YEAR OF WEEK
**year\_of\_week(x)** -\> bigint

Returns the year of the [ISO week](https://calendars.wikia.org/wiki/ISO_week_date) from `x`.

#### YOW
**yow(x)** -\> bigint

This is an alias for `year_of_week`.


[^1]: This specifier does not support `0` as a month or day.

[^2]: This specifier does not support `0` as a month or day.

[^3]: This specifier does not support `0` as a month or day.

[^4]: Timestamp is truncated to milliseconds.

[^5]: This specifier does not support `0` as a month or day.

[^6]: This specifier is not supported yet. Consider using `day_of_week` (it uses `1-7` instead of `0-6`).

[^7]: When parsing, two-digit year format assumes range `1970` .. `2069`, so \"70\" will result in year `1970` but \"69\" will produce `2069`.
