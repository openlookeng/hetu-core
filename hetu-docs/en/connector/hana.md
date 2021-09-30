
Hana Connector
==============

Overview
--------

The Hana connector allows querying and creating tables on an external Hana database. This can be used to join data between different systems like Hana and Hive, or between two different Hana instances.

Configurations
--------------

### Basic configuration

First of all, we should finish the following steps before you start to use hana connector.

-   JDBC Connection details to connect to the SAP HANA

It should be written in form of a regular openLooKeng connector config (eg. hana.properties for a openLooKeng catalog named hana). The file should contain the following contents, replacing the connection properties as appropriate for your setup.

Base property setting: 

| connector.name=hana                                          |
| ------------------------------------------------------------ |
| connection-url=jdbc:sap://HOST:PORT                          |
| connection-user=USERNAME                                     |
| connection-password=PASSWD                                   |
| allow-drop-table=true # allow hana connector to drop table or not |

-   Adding SAP HANA Driver

SAP HANA JDBC Driver is not available in common repositories, so you will need to download it from SAP HANA and install manually in your repository. The SAP HANA JDBC Driver (ngdbc.jar) may be installed as
part of your SAP HANA client installation or you can download it from the SAP HANA office website. Once you have got the SAP HANA JDBC Driver, you can deploy the jdbc jar file to openLooKeng plugin folder on coordinator and worker nodes. For example, if the jdbc driver file is ngdbc.jar and openLooKeng plugin folder is /usr/lib/openlookeng/lib/plugin, use the following command to copy the library to the plugin folder. cp ngdbc.jar
/usr/lib/openlookeng/lib/plugin/hana Restart the coordinator and worker processes and hana connector will work.

-   Enable the query push down feature or not.

If you want to enable the connector push down feature for hana connector, you do not need to do any things for hana connector\'s push down feature is turn on by default. But you can also set as below:

| jdbc.pushdown-enabled=true                   |
| --------------------------------------------------- |
| # true to enable pushdown, false to disable it. |

-   Mode for the push-down feature.

If you want to enable the connector all push down feature for hana connector, you do not need to do any things for hana connector's push down feature, which is FULL_PUSHDOWN on by default. But you can also set as below:

| jdbc.pushdown-module=FULL_PUSHDOWN                    |
| --------------------------------------------------- |
| # FULL_PUSHDOWN: All push down. BASE_PUSHDOWN: Partial push down, which indicates that filter, aggregation, limit, topN and project can be pushed down. |

### Multiple Hana Databases or Servers

Please configure another instance of the Hana plugin as a separate catalog if you want to connect to ultiple Hana Databases. To add another SAP HANA catalog, please add another properties file to ../conf/catalog with a different name (making sure it ends in .properties). For example, add a file named hana2.properties to ../conf/catalog to add another connector named hana2.

Querying Hana through openLooKeng
--------------------------

For there is a SAP HANA connector named hana, each SAP HANA Database\'s user can get its available schemas through the hana connector by running SHOW SCHEMAS:

    SHOW SCHEMAS FROM hana;

If you have, you can view the tables own by a SAP HANA Database named data by running SHOW TABLES:

    SHOW TABLES FROM hana.data;

To see a list of the columns in a table named hello in data\'s schema, use either of the following:

    DESCRIBE hana.data.hello;
    SHOW COLUMNS FROM hana.data.hello;

And you can access the hello table in data\'s schema:

    SELECT * FROM hana.data.hello;

The connector\'s privileges in these schemas are those of the user configured in the connection properties file. If the user does not have access to these tables, the specific connector will not be able to access them.

Mapping Data Types Between openLooKeng and Hana
----------------------------------------

### Hana-to-openLooKeng Type Mapping

openLooKeng support selecting the following SAP HANA Database types. The table shows the mapping from SAP HANA data type.

Data type projection table:

> | SAP HANA Detabase type | openLooKeng type                                              | Notes                    |
> | :--------------------- | :----------------------------------------------------- | :----------------------- |
> | DECIMAL(p, s)          | DECIMAL(p, s)                                          |                          |
> | SMALLDECIMAL           | DOUBLE                                                 | See smalldecimal mapping |
> | TINYINT                | TINYINT                                                |                          |
> | SMALLINT               | SMALLINT                                               |                          |
> | INTEGER                | INTEGER                                                |                          |
> | BIGINT                 | BIGINT                                                 |                          |
> | REAL                   | REAL                                                   |                          |
> | DOUBLE                 | DOUBLE                                                 |                          |
> | FLOAT(n)               | n<25 -> real, 25<=n<=53 or n is not declared -> double |                          |
> | BOOLEAN                | BOOLEAN                                                |                          |
> | VARCHAR                | VARCHAR                                                |                          |
> | NVARCHAR               | VARCHAR                                                |                          |
> | ALPHANUM               | CHAR                                                   |                          |
> | SHORTTEXT              | CHAR                                                   |                          |
> | VARBINARY              | VARBINARY                                              |                          |
> | DATE                   | DATE                                                   |                          |
> | TIME                   | TIME                                                   |                          |
> | TIMESTAMP              | TIMESTAMP                                              |                          |
> | SECONDDATE             | NA                                                     |      Not Available       |
> | BLOB                   | NA                                                     |      Not Available       |
> | CLOB                   | NA                                                     |      Not Available       |
> | NCLOB                  | NA                                                     |      Not Available       |
> | TEXT                   | NA                                                     |      Not Available       |
> | BINTEXT                | NA                                                     |      Not Available       |

**Note**

smalldecimal mapping: The arrange which do not cause loss of precision is the IEEE 754 double\'s exactly representable numbers. Detail in:
<https://en.wikipedia.org/wiki/Double-precision_floating-point_format>.


### openLooKeng-to-Hana Type Mapping

openLooKeng support creating tables with the following type into a SAP HANA Database. The table shows the mapping from openLooKeng to SAP HANA data types.

> | openLooKeng type                | SAP HANA Detabase type | Notes |
> | :----------------------- | :--------------------- | :---- |
> | BOOLEAN                  | BOOLEAN                |       |
> | TINYINT                  | TINYINT                |       |
> | SMALLINT                 | SMALLINT               |       |
> | INTEGER                  | INTEGER                |       |
> | BIGINT                   | BIGINT                 |       |
> | REAL                     | REAL                   |       |
> | DOUBLE                   | DOUBLE                 |       |
> | DECIMAL                  | DECIMAL                |       |
> | VARCHAR                  | VARCHAR                |       |
> | CHAR                     | CHAR                   |       |
> | VARBINARY                | VARBINARY              |       |
> | JSON                     | NA                     |   Not Available    |
> | DATE                     | DATE                   |       |
> | TIME                     | NA                   |  Not Available     |
> | TIME WITH TIME ZONE      | NA                     |  Not Available     |
> | TIMESTAMP                | NA              |   Not Available    |
> | TIMESTAMP WITH TIME ZONE | NA                     |  Not Available     |

### openLooKeng-to-Hana function Mapping

The openLooKeng functions which can be mapped to SAP HANA function is listed in the following table. Note: The \"\$n\" is placeholder to present an argument in a function.

> | openLooKeng function                |               HANA function | notes                                                        |
> | ----------------------------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
> | DATE_ADD(Unit, $1, $2)                                      | ADD_SECONDS($2, $1) or ADD_DAYS($2, $1) or ADD_MONTHS($2, $1) or ADD_YEARS($2, $1) | When unit is ‘second’,’minute’ or ‘hour’, mapping to ADD_SECONDS. When unit is ‘day’ or ‘week’, mapping to ADD_DAYS. When unit is ‘month’ or ‘quarter’, mapping to ADD_MONTHS. When unit is ‘year’, mapping to ADD_YEARS. |
> | CORR($1,$2)                                                 | CORR($1, $2)                                                 |                                                              |
> | STDDEV($1)                                                  | STDDEV($1)                                                   |                                                              |
> | VARIANCE($1)                                                | VAR($1)                                                      |                                                              |
> | ABS($1)                                                     | ABS($1)                                                      |                                                              |
> | ACOS($1)                                                    | ACOS($1)                                                     |                                                              |
> | ASIN($1)                                                    | ASIN($1)                                                     |                                                              |
> | ATAN($1)                                                    | ATAN($1)                                                     |                                                              |
> | ATAN2($1,$2)                                                | ATAN2($1, $2)                                                |                                                              |
> | CEIL($1)                                                    | CEIL($1)                                                     |                                                              |
> | CEILING($1)                                                 | CEIL($1)                                                     |                                                              |
> | COS($1)                                                     | COS($1)                                                      |                                                              |
> | EXP($1)                                                     | EXP($1)                                                      |                                                              |
> | FLOOR($1)                                                   | FLOOR($1)                                                    |                                                              |
> | LN($1)                                                      | LN($1)                                                       |                                                              |
> | LOG10($1)                                                   | LOG(10, $1)                                                  |                                                              |
> | LOG2($1)                                                    | LOG(2, $1)                                                   |                                                              |
> | LOG($1,$2)                                                  | LOG($1, $2)                                                  |                                                              |
> | MOD($1,$2)                                                  | MOD($1, $2)                                                  |                                                              |
> | POW($1,$2)                                                  | POW($1, $2)                                                  |                                                              |
> | POWER($1,$2)                                                | POWER($1, $2)                                                |                                                              |
> | RAND()                                                      | RAND()                                                       |                                                              |
> | RANDOM()                                                    | RAND()                                                       |                                                              |
> | ROUND($1)                                                   | ROUND($1)                                                    |                                                              |
> | ROUND($1,$2)                                                | ROUND($1, $2)                                                |                                                              |
> | SIGN($1)                                                    | SIGN($1)                                                     |                                                              |
> | SIN($1)                                                     | SIN($1)                                                      |                                                              |
> | SQRT($1)                                                    | SQRT($1)                                                     |                                                              |
> | TAN($1)                                                     | TAN($1)                                                      |                                                              |
> | CONCAT($1,$2)                                               | CONCAT($1, $2)                                               |                                                              |
> | LENGTH($1)                                                  | LENGTH($1)                                                   |                                                              |
> | LOWER($1)                                                   | LOWER($1)                                                    |                                                              |
> | LPAD($1,$2,$3)                                              | LPAD($1, $2, $3)                                             |                                                              |
> | LTRIM($1)                                                   | LTRIM($1)                                                    |                                                              |
> | REPLACE($1,$2)                                              | REPLACE($1, $2, ‘’)                                          |                                                              |
> | REPLACE($1,$2,$3)                                           | REPLACE($1, $2, $3)                                          |                                                              |
> | RPAD($1,$2,$3)                                              | RPAD($1, $2, $3)                                             |                                                              |
> | RTRIM($1)                                                   | RTRIM($1)                                                    |                                                              |
> | STRPOS($1,$2)                                               | LOCATE($1, $2)                                               |                                                              |
> | SUBSTR($1,$2,$3)                                            | SUBSTR($1, $2, $3)                                           |                                                              |
> | POSITION($1,$2)                                             | LOCATE($2, $1)                                               |                                                              |
> | TRIM($1)                                                    | TRIM($1)                                                     |                                                              |
> | UPPER($1)                                                   | UPPER($1)                                                    |                                                              |
> | YEAR($1)                                                    | EXTRACT(YEAR FROM $1)                                        |                                                              |
> | MONTH($1)                                                   | EXTRACT(MONTH FROM $1)                                       |                                                              |
> | DAY($1)                                                     | EXTRACT(DAY FROM $1)                                         |                                                              |
> | HOUR($1)                                                    | EXTRACT(HOUR FROM $1)                                        |                                                              |
> | MINUTE($1)                                                  | EXTRACT(MINUTE FROM $1)                                      |                                                              |
> | SECOND($1)                                                  | EXTRACT(SECOND FROM $1)                                      |                                                              |
> | DAY_OF_WEEK($1)                                             | WEEKDAY($1)                                                  |                                                              |


Hana sql migrate to openLooKeng sql guide
----------------------------------

### SQL grammar differences between hana and openLooKeng

Such as `map` function in hana sql which is used to transform the row data into column data is not support by openLooKeng sql. But you can use the `case` as an alternative implementation.

For example, if you have a table named SCORES:

| name | course  | score |
| :--- | :------ | :---- |
| zs   | English | 90    |
| zs   | Math    | 80    |
| zs   | science | 99    |
| ls   | Math    | 80    |
| ls   | science | 99    |
| ls   | English | 90    |
| ww   | science | 99    |
| ww   | Math    | 80    |
| ww   | English | 90    |

In hana you can use `map` function to transform the row data into column data:

```sql
SELECT
  NAME,
  SUM(MAP(SUBJECT,'English',SCORE,0)) AS "English",
  SUM(MAP(SUBJECT,'Math',SCORE,0)) AS "Math",
  SUM(MAP(SUBJECT,'Science',SCORE,0)) AS "Science"
FROM SCORES
GROUP BY NAME
```

In openLookeng, you can use `case` as an alternative implementation:

The other differences between hana and openLooKeng sql grammar, please refer to the official document list below:

| name        | web address                                                                                                    |
| :-----------| :------------------------------------------------------------------------------------------------------------- |
| openLooKeng | [SQL Grammar](../sql/_index.md)                                                                                  |
| hana        | [SQL Grammar](https://help.sap.com/viewer/7c78579ce9b14a669c1f3295b0d8ca16/Cloud/en-US/20ff532c751910148657c32fe3431a9f.html) |

### Time dependent type\'s Differences

When we use the openlk-cli to connect the openLooKeng server and handle the time and timestamp without time zone, the result return to the cli to display will depend on the openlk-cli\'s start up configuration. For example, if we start up the openlk-cli with user timezone as:

    java -jar -Duser.timezone=Asia/Tokyo -jar ./hetu-cli-*.jar
    --client-request-timeout 30m --server ip:8080 --session legacy_timestamp=false

When you handle time and timestamp dependent types, the openlk-cli will display Time dependent type with time zone:

```sql
lk> select current_time;
                  _col0
          -------------------------
           21:19:49.122 Asia/Tokyo
lk> select current_timezone();
              _col0
           ------------
            Asia/Tokyo
             (1 row)
```

If we start up the openlk-cli without user timezone as:

```shell
java -jar ./hetu-cli-*.jar
--client-request-timeout 30m --server ip:8080
--session legacy_timestamp=false --catalog hana2
```

When you handle time and timestamp dependent types, the openlk-cli will display Time dependent type without time zone. Instead, it will display
the UTC/GMT zone:

```sql
lk> select current_timezone();
          _col0
        --------
         +08:00
         (1 row)
lk> select current_time;
          _col0
          ---------------------
          20:20:45.659 +08:00
          (1 row)
```

But in hana, the time dependent type\'s behavior is depend on the hana server. For example, we start up a hana client through jdbc directly:

```shell
java -jar -Duser.timezone=Asia/Tokyo ngdbc.jar -u
database,passwd -n ip:34215 -c "SELECT CURRENT_TIME FROM DUMMY"
       Connected.
       |          |
       ------------
       | 20:38:57 |
```

Hana Connector\'s Limitations
-----------------------------

For the differences between hana datatype and openLooKeng datatype, there are some limitations when project the hana data type to the openLooKeng datatype.

### Hana\'s Smalldecimal Data type

The smalldecimal in hana has a variable precision and zero scale length, but openLooKeng do not support. For the reason about, the openLooKeng translate smalldecimal in hana into double in openLooKeng, and this will cause some
precision lose in a special value range.

### Hana\'s tiny int data type

Tiny int in hana is a 8 bits integer without sign bit and will cause some precision lose in a special value range in openLooKeng.
