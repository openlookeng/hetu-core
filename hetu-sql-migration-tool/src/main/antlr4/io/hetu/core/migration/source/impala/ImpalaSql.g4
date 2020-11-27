/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

grammar ImpalaSql;

tokens {
    DELIMITER
}

singleStatement
    : statement EOF
    ;

standaloneExpression
    : expression EOF
    ;

standalonePathSpecification
    : pathSpecification EOF
    ;

statement
    : query                                                            #statementDefault
    | USE schema=identifier                                            #use
    | CREATE (SCHEMA | DATABASE) (IF NOT EXISTS)? qualifiedName
        (COMMENT comment=string)? (LOCATION location=string)?                            #createSchema
    | ALTER DATABASE qualifiedName SET OWNER (USER | ROLE) identifier         #alterSchema
    | DROP (SCHEMA | DATABASE) (IF EXISTS)? qualifiedName (CASCADE | RESTRICT)?     #dropSchema
    | CREATE EXTERNAL? TABLE (IF NOT EXISTS)? tblName=qualifiedName
            ('(' tableElement (',' tableElement)* ')')?
            (PARTITIONED BY '('partitionedBy')')?
            (SORT BY '(' sortedBy ')')?
            (COMMENT comment=string)?
            (ROW FORMAT rowFormat)?
            (WITH SERDEPROPERTIES serdProp=properties)?
            (STORED_AS stored_as=identifier)?
            (LOCATION location=string)?
            (CACHED IN cacheName=qualifiedName (WITH REPLICATION EQ INTEGER_VALUE)? | UNCACHED)?
            (TBLPROPERTIES tblProp=properties)?
            (AS query)?                                                         #createTable
    | CREATE EXTERNAL? TABLE (IF NOT EXISTS)? tblName=qualifiedName
        LIKE (likeTableName=qualifiedName | PARQUET parquet=string)
        (COMMENT comment=string)?
        (STORED_AS stored_as=identifier)?
        (LOCATION location=string)?                                             #createTableLike
    | CREATE EXTERNAL? TABLE (IF NOT EXISTS)? tblName=qualifiedName
        ('(' kuduTableElement (',' kuduTableElement)* (',' PRIMARY KEY columnAliases)? ')')?
        (PARTITION BY .*)?
        (COMMENT string)?
        STORED_AS KUDU
        (TBLPROPERTIES tblProp=properties)?                            #createKuduTable
    | CREATE EXTERNAL? TABLE (IF NOT EXISTS)? tblName=qualifiedName
        ('(' PRIMARY KEY columnAliases? ')')?
        (PARTITION BY .*)?
        (COMMENT string)?
        STORED_AS KUDU
        (TBLPROPERTIES tblProp=properties)?
        AS query                                                       #createKuduTableAsSelect
    | ALTER TABLE from=qualifiedName RENAME TO to=qualifiedName        #renameTable
    | ALTER TABLE qualifiedName ADD (IF NOT EXISTS)? COLUMNS '(' columnSpecWithKudu (',' columnSpecWithKudu)* ')'        #addColumns
    | ALTER TABLE qualifiedName REPLACE COLUMNS '(' columnSpecWithKudu (',' columnSpecWithKudu)* ')'        #replaceColumns
    | ALTER TABLE qualifiedName ADD COLUMN (IF NOT EXISTS)? columnSpecWithKudu        #addSingleColumn
    | ALTER TABLE qualifiedName DROP (COLUMN)? identifier        #dropSingleColumn
    | ALTER TABLE qualifiedName SET OWNER (USER | ROLE) identifier        #alterTableOwner
    | ALTER TABLE qualifiedName ALTER (COLUMN)? identifier '{' (SET expression expression | DROP DEFAULT) '}'        #alterTableKuduOnly
    | DROP TABLE (IF EXISTS)? qualifiedName PURGE?                     #dropTable
    | TRUNCATE TABLE? (IF EXISTS)? qualifiedName                       #truncateTable
    | CREATE VIEW (IF NOT EXISTS)?  qualifiedName viewColumns?
            (COMMENT string)?
            AS query                                                    #createView
    | ALTER VIEW qualifiedName viewColumns?
            AS query                                                    #alterView
    | ALTER VIEW qualifiedName RENAME TO qualifiedName                  #renameView
    | ALTER VIEW qualifiedName SET OWNER (USER|ROLE) qualifiedName                  #alterViewOwner
    | DROP VIEW (IF EXISTS)? qualifiedName                             #dropView
    | DESCRIBE DATABASE? (FORMATTED|EXTENDED)? qualifiedName           #describeDbOrTable
    | COMPUTE STATS qualifiedName  (columnAliases)? (TABLESAMPLE SYSTEM '('number')' (REPEATABLE'('number')')?)?    #computeStats
    | COMPUTE INCREMENTAL STATS qualifiedName (PARTITION expression)?                                                        #computeIncrementalStats
    | DROP STATS qualifiedName                                          #dropStats
    | DROP INCREMENTAL STATS qualifiedName PARTITION '('expression')'         #dropIncrementalStats
    | CREATE AGGREGATE? FUNCTION (IF NOT EXISTS)? qualifiedName ('('(type (',' type)*)? ')')?
             (RETURNS type)?
             (INTERMEDIATE type)?
             LOCATION STRING
             (SYMBOL EQ symbol=string)?
             (INIT_FN EQ STRING)?
             (UPDATE_FN EQ STRING)?
             (MERGE_FN EQ STRING)?
             (CLOSEFN EQ STRING)?
             (SERIALIZE_FN EQ STRING)?
             (FINALIZE_FN EQ STRING)?                                  #createFunction
    | REFRESH FUNCTIONS qualifiedName                                 #refreshFunction
    | DROP AGGREGATE? FUNCTION (IF EXISTS)? qualifiedName ('('(type (',' type)*)? ')')?       #dropFunction
    | CREATE ROLE name=identifier                                      #createRole
    | DROP ROLE name=identifier                                        #dropRole
    | GRANT ROLE identifier TO GROUP identifier                        #grantRole
    | GRANT (privilege (',' privilege)* | ALL)
            ON objectType qualifiedName TO grantee=principal (WITH GRANT OPTION)?            #grant
    | REVOKE ROLE identifier FROM GROUP identifier                                                   #revokeRole
    | REVOKE (GRANT OPTION FOR)? (privilege (',' privilege)* | ALL)
            ON objectType qualifiedName FROM grantee=principal          #revoke
    | with? INSERT hintClause? (INTO | OVERWRITE) TABLE? qualifiedName
            columnAliases?
            (PARTITION '('expression(',' expression)*')')?
            hintClause? query                                           #insertInto
    | DELETE FROM? qualifiedName (WHERE booleanExpression)?                                   #delete
    | DELETE expression (AS? identifier)? FROM? relation ((',' relation)*)? (WHERE booleanExpression)?                                   #deleteTableRef
    | UPDATE qualifiedName SET assignmentList (FROM relation (',' relation)*)? (WHERE booleanExpression)?    #updateTable
    | UPSERT hintClause? INTO TABLE? qualifiedName
             columnAliases?
             hintClause? query                                          #upsert
    | SHOW (SCHEMAS | DATABASES)
             (LIKE? pattern=string ('|' string)*)?                                         #showSchemas
    | SHOW TABLES ((FROM | IN) qualifiedName)?
             (LIKE? pattern=string ('|' string)*)?                                        #showTables
    | SHOW (AGGREGATE | ANALYTIC)? FUNCTIONS (IN qualifiedName)?
             (LIKE? pattern=string ('|' string)*)?                                  #showFunctions
    | SHOW CREATE TABLE qualifiedName                                  #showCreateTable
    | SHOW CREATE VIEW qualifiedName                                  #showCreateView
    | SHOW TABLE STATS qualifiedName                                                      #showTableStats
    | SHOW COLUMN STATS qualifiedName                                                     #showColumnStats
    | SHOW (RANGE)? PARTITIONS qualifiedName                                              #showPartitions
    | SHOW FILES IN qualifiedName (PARTITION '('expression (',' expression)?')')?         #showFiles
    | SHOW (CURRENT)? ROLES                                                               #showRoles
    | SHOW ROLE GRANT GROUP identifier                                                    #showRoleGrant
    | SHOW GRANT ROLE identifier                                                          #showGrantRole
    | SHOW GRANT USER identifier
        (ON (SERVER | DATABASE | TABLE | URI) (qualifiedName)? )?                         #showGrantUser
    | COMMENT ON (DATABASE|TABLE|COLUMN) qualifiedName IS (string | NULL)                 #addComments
    | EXPLAIN statement                                                                   #explain
    | SET (ALL | identifier EQ expression)?                                               #setSession
    | ':'SHUTDOWN '(' ('\\')? (expression)? (':' expression)? (',' expression )? ')'                  #shutdown
    | INVALIDATE METADATA qualifiedName                                                   #invalidateMeta
    | LOAD DATA INPATH STRING (OVERWRITE)? INTO TABLE qualifiedName
        (PARTITION '('expression (',' expression)?')')?                                   #loadData
    | REFRESH qualifiedName (PARTITION '('expression (',' expression)?')')?               #refreshMeta
    | REFRESH AUTHORIZATION                                                               #refreshAuth
    ;

assignmentList
    :  assignmentItem (',' assignmentItem)*
    ;

assignmentItem
    :  qualifiedName EQ expression
    ;

viewColumns
    : '(' identifier (COMMENT string)? (',' identifier (COMMENT string)?)* ')'
    ;

query
    :  with? queryNoWith
    ;

with
    : WITH namedQuery (',' namedQuery)*
    ;

tableElement
    : columnDefinition
    ;

columnDefinition
    : identifier type (COMMENT string)?
    ;

kuduTableElement
    : kuduColumnDefinition
    ;

kuduColumnDefinition
    : identifier type (kuduAttributes)? (COMMENT string)? (PRIMARY KEY)?
    ;

columnSpecWithKudu
    : identifier type (COMMENT string)? (kuduAttributes)?
    ;
kuduAttributes
    : '{' ((NOT)? NULL | ENCODING expression | COMPRESSION expression | DEFAULT expression | BLOCK_SIZE number) '}'
    ;

hintClause
    : '-- +SHUFFLE' | '-- +NOSHUFFLE -- +CLUSTERED'
    | '/* +SHUFFLE */' | '/* +NOSHUFFLE */' | '/* +CLUSTERED */'
    | '[SHUFFLE]' | '[NOSHUFFLE]'
    ;

properties
    : '(' property (',' property)* ')'
    ;

partitionedBy
    : columnDefinition (',' columnDefinition)*
    ;

sortedBy
    : expression (',' expression)*
    ;

rowFormat
    : DELIMITED (FIELDS TERMINATED BY string (ESCAPED BY string)?)? (LINES TERMINATED BY string)?
    ;

property
    : identifier EQ expression
    ;

queryNoWith:
      queryTerm
      (ORDER BY sortItem (',' sortItem)*)?
      (LIMIT rows=INTEGER_VALUE (OFFSET offset=INTEGER_VALUE)?)?
    ;

queryTerm
    : queryPrimary                                                             #queryTermDefault
    | left=queryTerm operator=INTERSECT setQuantifier? right=queryTerm         #setOperation
    | left=queryTerm operator=(UNION | EXCEPT) setQuantifier? right=queryTerm  #setOperation
    ;

queryPrimary
    : querySpecification                   #queryPrimaryDefault
    | TABLE qualifiedName                  #table
    | VALUES expression (',' expression)*  #inlineTable
    | '(' queryNoWith  ')'                 #subquery
    ;

sortItem
    : expression ordering=(ASC | DESC)? (NULLS nullOrdering=(FIRST | LAST))?
    ;

querySpecification
    : SELECT setQuantifier? (STRAIGHT_JOIN)? selectItem (',' selectItem)*
      (FROM relation (',' relation)*)?
      (WHERE where=booleanExpression)?
      (GROUP BY groupBy)?
      (HAVING having=booleanExpression)?
    ;

groupBy
    : setQuantifier? groupingElement (',' groupingElement)*
    ;

groupingElement
    : groupingSet                                            #singleGroupingSet
    ;

groupingSet
    : '(' (expression (',' expression)*)? ')'
    | expression
    ;

namedQuery
    : name=identifier (columnAliases)? AS '(' query ')'
    ;

setQuantifier
    : DISTINCT
    | ALL
    ;

selectItem
    : expression (AS? identifier)?  #selectSingle
    | qualifiedName '.' ASTERISK    #selectAll
    | ASTERISK                      #selectAll
    ;

relation
    : left=relation
      ( CROSS JOIN right=sampledRelation
      | joinType JOIN rightRelation=relation joinCriteria
      )                                           #joinRelation
    | sampledRelation                             #relationDefault
    ;

joinType
    : INNER?
    | LEFT INNER?
    | RIGHT INNER?
    | LEFT OUTER?
    | RIGHT OUTER?
    | FULL OUTER?
    | LEFT SEMI
    | RIGHT SEMI
    | LEFT ANTI
    | RIGHT ANTI
    ;

joinCriteria
    : ON booleanExpression
    | USING '(' identifier (',' identifier)* ')'
    ;

sampledRelation
    : aliasedRelation (
        TABLESAMPLE sampleType '(' percentage=expression ')'
      )?
    ;

sampleType
    : BERNOULLI
    | SYSTEM
    ;

aliasedRelation
    : relationPrimary (AS? identifier columnAliases?)?
    ;

columnAliases
    : '(' identifier (',' identifier)* ')'
    ;

relationPrimary
    : qualifiedName                                                   #tableName
    | '(' query ')'                                                   #subqueryRelation
    | UNNEST '(' expression (',' expression)* ')' (WITH ORDINALITY)?  #unnest
    | LATERAL '(' query ')'                                           #lateral
    | '(' relation ')'                                                #parenthesizedRelation
    ;

expression
    : booleanExpression
    ;

booleanExpression
    : valueExpression predicate[$valueExpression.ctx]?             #predicated
    | NOT booleanExpression                                        #logicalNot
    | left=booleanExpression operator=AND right=booleanExpression  #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression   #logicalBinary
    ;

// workaround for https://github.com/antlr/antlr4/issues/780
predicate[ParserRuleContext value]
    : comparisonOperator right=valueExpression                            #comparison
    | comparisonOperator comparisonQuantifier '(' query ')'               #quantifiedComparison
    | NOT? BETWEEN lower=valueExpression AND upper=valueExpression        #between
    | NOT? IN '(' expression (',' expression)* ')'                        #inList
    | NOT? IN '(' query ')'                                               #inSubquery
    | NOT? LIKE pattern=valueExpression (ESCAPE escape=valueExpression)?  #like
    | IS NOT? NULL                                                        #nullPredicate
    | IS NOT? DISTINCT FROM right=valueExpression                         #distinctFrom
    ;

valueExpression
    : primaryExpression                                                                 #valueExpressionDefault
    | operator=(MINUS | PLUS) valueExpression                                           #arithmeticUnary
    | left=valueExpression operator=(ASTERISK | SLASH | PERCENT) right=valueExpression  #arithmeticBinary
    | left=valueExpression operator=(PLUS | MINUS) right=valueExpression                #arithmeticBinary
    | left=valueExpression CONCAT right=valueExpression                                 #concatenation
    ;

primaryExpression
    : NULL                                                                                #nullLiteral
    | interval                                                                            #intervalLiteral
    | identifier string                                                                   #typeConstructor
    | DOUBLE_PRECISION string                                                             #typeConstructor
    | number                                                                              #numericLiteral
    | booleanValue                                                                        #booleanLiteral
    | string                                                                              #stringLiteral
    | BINARY_LITERAL                                                                      #binaryLiteral
    | '?'                                                                                 #parameter
    | POSITION '(' valueExpression IN valueExpression ')'                                 #position
    | '(' expression (',' expression)+ ')'                                                #rowConstructor
    | ROW '(' expression (',' expression)* ')'                                            #rowConstructor
    | qualifiedName '(' ASTERISK ')' filter? over?                                        #functionCall
    | qualifiedName '(' (setQuantifier? expression (',' expression)*)?
        (ORDER BY sortItem (',' sortItem)*)? ')' filter? over?                            #functionCall
    | identifier '->' expression                                                          #lambda
    | '(' (identifier (',' identifier)*)? ')' '->' expression                             #lambda
    | '(' query ')'                                                                       #subqueryExpression
    // This is an extension to ANSI SQL, which considers EXISTS to be a <boolean expression>
    | EXISTS '(' query ')'                                                                #exists
    | CASE valueExpression whenClause+ (ELSE elseExpression=expression)? END              #simpleCase
    | CASE whenClause+ (ELSE elseExpression=expression)? END                              #searchedCase
    | CAST '(' expression AS type ')'                                                     #cast
    | TRY_CAST '(' expression AS type ')'                                                 #cast
    | ARRAY '[' (expression (',' expression)*)? ']'                                       #arrayConstructor
    | value=primaryExpression '[' index=valueExpression ']'                               #subscript
    | identifier                                                                          #columnReference
    | base=primaryExpression '.' fieldName=identifier                                     #dereference
    | name=CURRENT_TIMESTAMP '(' ')'                                                      #specialDateTimeFunction
    | SUBSTRING '(' valueExpression FROM valueExpression (FOR valueExpression)? ')'       #substring
    | NORMALIZE '(' valueExpression (',' normalForm)? ')'                                 #normalize
    | EXTRACT '(' identifier FROM valueExpression ')'                                     #extract
    | '(' expression ')'                                                                  #parenthesizedExpression
    | GROUPING '(' (qualifiedName (',' qualifiedName)*)? ')'                              #groupingOperation
    ;

string
    : STRING                                #basicStringLiteral
    ;

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

comparisonQuantifier
    : ALL | SOME | ANY
    ;

booleanValue
    : TRUE | FALSE
    ;

interval
    : INTEGER_VALUE intervalField
    | '(' INTEGER_VALUE ')' intervalField
    | INTERVAL INTEGER_VALUE intervalField
    | INTERVAL '(' INTEGER_VALUE ')' intervalField
    ;

intervalField
    : YEAR | YEARS | MONTH | MONTHS | DAY | DAYS | HOUR | HOURS | MINUTE | MINUTES | SECOND | SECONDS
    ;

normalForm
    : NFD | NFC | NFKD | NFKC
    ;

type
    : type ARRAY
    | ARRAY '<' type '>'
    | MAP '<' type ',' type '>'
    | STRUCT '<' identifier ':' type (',' identifier ':' type)* '>'
    | baseType ('(' typeParameter (',' typeParameter)* ')')?
    ;

typeParameter
    : INTEGER_VALUE | type
    ;

baseType
    : TIME_WITH_TIME_ZONE
    | TIMESTAMP_WITH_TIME_ZONE
    | DOUBLE_PRECISION
    | identifier
    ;

whenClause
    : WHEN condition=expression THEN result=expression
    ;

filter
    : FILTER '(' WHERE booleanExpression ')'
    ;

over
    : OVER '('
        (PARTITION BY partition+=expression (',' partition+=expression)*)?
        (ORDER BY sortItem (',' sortItem)*)?
        windowFrame?
      ')'
    ;

windowFrame
    : frameType=RANGE start=frameBound
    | frameType=ROWS start=frameBound
    | frameType=RANGE BETWEEN start=frameBound AND end=frameBound
    | frameType=ROWS BETWEEN start=frameBound AND end=frameBound
    ;

frameBound
    : UNBOUNDED boundType=PRECEDING                 #unboundedFrame
    | UNBOUNDED boundType=FOLLOWING                 #unboundedFrame
    | CURRENT ROW                                   #currentRowBound
    | expression boundType=(PRECEDING | FOLLOWING)  #boundedFrame
    ;

pathElement
    : identifier '.' identifier     #qualifiedArgument
    | identifier                    #unqualifiedArgument
    ;

pathSpecification
    : pathElement (',' pathElement)*
    ;

privilege
    : CREATE | INSERT | REFRESH | SELECT ('('columnName=identifier')')?
    ;
objectType
    : SERVER | URI | DATABASE | TABLE
    ;
qualifiedName
    : identifier ('.' identifier)*
    ;

principal
    : identifier            #unspecifiedPrincipal
    | ROLE identifier       #rolePrincipal
    ;

identifier
    : IDENTIFIER             #unquotedIdentifier
    | STRING                 #quotedIdentifier
    | nonReserved            #unquotedIdentifier
    | BACKQUOTED_IDENTIFIER  #backQuotedIdentifier
    | DIGIT_IDENTIFIER       #digitIdentifier
    ;

number
    : MINUS? DECIMAL_VALUE  #decimalLiteral
    | MINUS? DOUBLE_VALUE   #doubleLiteral
    | MINUS? INTEGER_VALUE  #integerLiteral
    ;

nonReserved
    // IMPORTANT: this rule must only contain tokens. Nested rules are not supported. See SqlParser.exitNonReserved
    : ADD | ADMIN | ALL | ANALYZE | ANY | ARRAY | ASC | AT
    | BERNOULLI
    | CALL | CASCADE | CATALOGS | COLUMN | COLUMNS | COMMENT | COMMIT | COMMITTED | CURRENT
    | DATA | DATABASE | DATABASES | DATE | DAY | DAYS | DEFINER | DESC
    | EXCLUDING | EXPLAIN
    | FETCH | FILTER | FIRST | FOLLOWING | FORMAT | FUNCTIONS
    | GRANT | GRANTED | GRANTS | GRAPHVIZ
    | HOUR
    | IF | INCLUDING | INPUT | INTERVAL | INVOKER | IO | ISOLATION
    | JSON
    | LAST | LATERAL | LEVEL | LIMIT | LOGICAL
    | MAP | MINUTE | MONTH
    | NEXT | NFC | NFD | NFKC | NFKD | NO | NONE | NULLIF | NULLS
    | OFFSET | ONLY | OPTION | ORDINALITY | OUTPUT | OVER
    | PARTITION | PARTITIONS | PARQUET | PATH | POSITION | PRECEDING | PRIVILEGES | PROPERTIES
    | RANGE | READ | RENAME | REPEATABLE | REPLACE | RESET | RESTRICT | REVOKE | ROLE | ROLES | ROLLBACK | ROW | ROWS
    | SCHEMA | SCHEMAS | SECOND | SECONDS | SECURITY | SERIALIZABLE | SESSION | SET | SETS
    | SHOW | SOME | START | STATS | SUBSTRING | SYSTEM
    | TABLES | TABLESAMPLE | TEXT | TIES | TIME | TIMESTAMP | TO | TRANSACTION | TRY_CAST | TYPE
    | UNBOUNDED | UNCOMMITTED | USE | USER
    | VALIDATE | VERBOSE | VIEW | VIEWS
    | WORK | WRITE
    | YEAR
    | ZONE
    | DEFAULT
    ;

ADD: 'ADD';
ADMIN: 'ADMIN';
ALL: 'ALL';
ALTER: 'ALTER';
ANALYZE: 'ANALYZE';
ANALYTIC: 'ANALYTIC';
AND: 'AND';
ANY: 'ANY';
ANTI: 'ANTI';
ARCHIVE: 'ARCHIVE';
ARRAY: 'ARRAY';
AS: 'AS';
ASC: 'ASC';
AT: 'AT';
AGGREGATE: 'AGGREGATE';
AUTHORIZATION: 'AUTHORIZATION';
BERNOULLI: 'BERNOULLI';
BETWEEN: 'BETWEEN';
PARTITIONED: 'PARTITIONED';
PREPARE_FN: 'PREPARE_FN';
TEMPORARY: 'TEMPORARY';
EXTERNAL: 'EXTERNAL';
CLOSEFN: 'CLOSEFN';
SORT: 'SORT';
SORTED: 'SORTED';
BUCKETS: 'BUCKETS';
PURGE: 'PURGE';
STORED: 'STORED';
STORED_AS: 'STORED AS';
LOCATION: 'LOCATION';
TBLPROPERTIES: 'TBLPROPERTIES';
DBPROPERTIES : 'DBPROPERTIES';
BY: 'BY';
CALL: 'CALL';
CASCADE: 'CASCADE';
CASE: 'CASE';
CAST: 'CAST';
CACHED: 'CACHED';
CATALOGS: 'CATALOGS';
COLUMN: 'COLUMN';
COLUMNS: 'COLUMNS';
COMMENT: 'COMMENT';
COMMIT: 'COMMIT';
COMMITTED: 'COMMITTED';
COMPUTE: 'COMPUTE';
CONSTRAINT: 'CONSTRAINT';
CREATE: 'CREATE';
CROSS: 'CROSS';
CUBE: 'CUBE';
CURRENT: 'CURRENT';
CURRENT_DATE: 'CURRENT_DATE';
CURRENT_PATH: 'CURRENT_PATH';
CURRENT_ROLE: 'CURRENT_ROLE';
CURRENT_TIME: 'CURRENT_TIME';
CURRENT_TIMESTAMP: 'CURRENT_TIMESTAMP';
CURRENT_USER: 'CURRENT_USER';
DATA: 'DATA';
DATABASE: 'DATABASE';
DATABASES: 'DATABASES';
DATE: 'DATE';
DAY: 'DAY';
DAYS: 'DAYS';
DEALLOCATE: 'DEALLOCATE';
DEFINER: 'DEFINER';
DELETE: 'DELETE';
DEFAULT: 'DEFAULT';
DELIMITED: 'DELIMITED ';
DISABLE: 'DISABLE';
UPDATE: 'UPDATE';
DESC: 'DESC';
DESCRIBE: 'DESCRIBE';
DISTINCT: 'DISTINCT';
DROP: 'DROP';
ELSE: 'ELSE';
ENABLE: 'ENABLE';
END: 'END';
ESCAPE: 'ESCAPE';
ESCAPED: 'ESCAPED';
EXCEPT: 'EXCEPT';
EXCLUDING: 'EXCLUDING';
EXECUTE: 'EXECUTE';
EXISTS: 'EXISTS';
EXPLAIN: 'EXPLAIN';
EXTRACT: 'EXTRACT';
EXTENDED: 'EXTENDED';
FALSE: 'FALSE';
FETCH: 'FETCH';
FIELDS: 'FIELDS';
FILE: 'FILE';
FILES: 'FILES';
FILTER: 'FILTER';
FIRST: 'FIRST';
FINALIZE_FN: 'FINALIZE_FN';
FOLLOWING: 'FOLLOWING';
FOR: 'FOR';
FORMAT: 'FORMAT';
FORMATTED: 'FORMATTED';
FROM: 'FROM';
FULL: 'FULL';
FUNCTION: 'FUNCTION';
FUNCTIONS: 'FUNCTIONS';
GRANT: 'GRANT';
GRANTED: 'GRANTED';
GRANTS: 'GRANTS';
GRAPHVIZ: 'GRAPHVIZ';
GROUP: 'GROUP';
GROUPING: 'GROUPING';
HASH: 'HASH';
HAVING: 'HAVING';
HOUR: 'HOUR';
HOURS: 'HOURS';
IF: 'IF';
IN: 'IN';
INCLUDING: 'INCLUDING';
INCREMENTAL: 'INCREMENTAL';
INNER: 'INNER';
INPATH: 'INPATH';
INPUT: 'INPUT';
INSERT: 'INSERT';
INTERSECT: 'INTERSECT';
INTERVAL: 'INTERVAL';
INTERMEDIATE: 'INTERMEDIATE';
INTO: 'INTO';
INVOKER: 'INVOKER';
INIT_FN: 'INIT_FN';
INVALIDATE: 'INVALIDATE';
IO: 'IO';
IS: 'IS';
ISOLATION: 'ISOLATION';
JAR: 'JAR';
JSON: 'JSON';
JOIN: 'JOIN';
KEY: 'KEY';
KUDU: 'KUDU';
LAST: 'LAST';
LATERAL: 'LATERAL';
LEFT: 'LEFT';
LEVEL: 'LEVEL';
LIKE: 'LIKE';
LIMIT: 'LIMIT';
LINES: 'LINES';
LOAD: 'LOAD';
LOCALTIME: 'LOCALTIME';
LOCALTIMESTAMP: 'LOCALTIMESTAMP';
LOGICAL: 'LOGICAL';
METADATA: 'METADATA';
MATERIALIZED: 'MATERIALIZED';
MAP: 'MAP';
MINUTE: 'MINUTE';
MINUTES: 'MINUTES';
MONTH: 'MONTH';
MONTHS: 'MONTHS';
NATURAL: 'NATURAL';
MERGE_FN: 'MERGE_FN';
NEXT: 'NEXT';
NFC : 'NFC';
NFD : 'NFD';
NFKC : 'NFKC';
NFKD : 'NFKD';
NO: 'NO';
NONE: 'NONE';
NORMALIZE: 'NORMALIZE';
NOT: 'NOT';
NULL: 'NULL';
NULLIF: 'NULLIF';
NULLS: 'NULLS';
OFFSET: 'OFFSET';
ON: 'ON';
ONLY: 'ONLY';
OPTION: 'OPTION';
OR: 'OR';
ORDER: 'ORDER';
ORDINALITY: 'ORDINALITY';
OUTER: 'OUTER';
OUTPUT: 'OUTPUT';
OWNER: 'OWNER';
OVER: 'OVER';
OVERWRITE: 'OVERWRITE';
PARTITION: 'PARTITION';
PARTITIONS: 'PARTITIONS';
PATH: 'PATH';
PARQUET: 'PARQUET';
POSITION: 'POSITION';
PRECEDING: 'PRECEDING';
PREPARE: 'PREPARE';
PRIMARY: 'PRIMARY';
REPLICATION: 'REPLICATION';
PRIVILEGES: 'PRIVILEGES';
PROPERTIES: 'PROPERTIES';
RANGE: 'RANGE';
READ: 'READ';
RELOAD: 'RELOAD';
RECURSIVE: 'RECURSIVE';
RENAME: 'RENAME';
REPEATABLE: 'REPEATABLE';
REPLACE: 'REPLACE';
REWRITE: 'REWRITE';
RESET: 'RESET';
RESTRICT: 'RESTRICT';
RETURNS: 'RETURNS';
REVOKE: 'REVOKE';
REFRESH: 'REFRESH';
RIGHT: 'RIGHT';
ROLE: 'ROLE';
ROLES: 'ROLES';
ROLLBACK: 'ROLLBACK';
ROLLUP: 'ROLLUP';
ROW: 'ROW';
ROWS: 'ROWS';
SCHEMA: 'SCHEMA';
SCHEMAS: 'SCHEMAS';
SECOND: 'SECOND';
SECONDS: 'SECONDS';
SECURITY: 'SECURITY';
SELECT: 'SELECT';
SERDEPROPERTIES: 'SERDEPROPERTIES';
SERIALIZABLE: 'SERIALIZABLE';
SESSION: 'SESSION';
SET: 'SET';
SETS: 'SETS';
SEMI: 'SEMI';
SERVER: 'SERVER';
SHOW: 'SHOW';
SHUTDOWN: 'SHUTDOWN';
SOME: 'SOME';
START: 'START';
STATS: 'STATS';
STRUCT: 'STRUCT';
STRAIGHT_JOIN: 'STRAIGHT_JOIN';
SUBSTRING: 'SUBSTRING';
SYSTEM: 'SYSTEM';
SYMBOL: 'SYMBOL';
SERIALIZE_FN: 'SERIALIZE_FN';
TABLE: 'TABLE';
TABLES: 'TABLES';
TABLESAMPLE: 'TABLESAMPLE';
TEXT: 'TEXT';
TERMINATED: 'TERMINATED ';
THEN: 'THEN';
TIES: 'TIES';
TIME: 'TIME';
TIMESTAMP: 'TIMESTAMP';
TO: 'TO';
TRANSACTION: 'TRANSACTION';
TRUE: 'TRUE';
TRY_CAST: 'TRY_CAST';
TRUNCATE: 'TRUNCATE';
TYPE: 'TYPE';
UNCACHED: 'UNCACHED';
UESCAPE: 'UESCAPE';
UNBOUNDED: 'UNBOUNDED';
UNCOMMITTED: 'UNCOMMITTED';
UNION: 'UNION';
UNNEST: 'UNNEST';
USE: 'USE';
USER: 'USER';
USING: 'USING';
UPDATE_FN: 'UPDATE_FN';
UPSERT: 'UPSERT';
URI: 'URI';
VALIDATE: 'VALIDATE';
VALUES: 'VALUES';
VERBOSE: 'VERBOSE';
VIEW: 'VIEW';
VIEWS: 'VIEWS';
WHEN: 'WHEN';
WHERE: 'WHERE';
WITH: 'WITH';
WORK: 'WORK';
WRITE: 'WRITE';
YEAR: 'YEAR';
YEARS: 'YEARS';
ZONE: 'ZONE';

EQ  : '=';
NEQ : '<>' | '!=';
LT  : '<';
LTE : '<=';
GT  : '>';
GTE : '>=';

PLUS: '+';
MINUS: '-';
ASTERISK: '*';
SLASH: '/';
PERCENT: '%';
CONCAT: '||';

STRING
    : '\'' (~'\'' | '\\' | ('\\' '\''))* '\''
    | '"' (~'"' | '\\' | '\\"')* '"'
    ;

// Note: we allow any character inside the binary literal and validate
// its a correct literal when the AST is being constructed. This
// allows us to provide more meaningful error messages to the user
BINARY_LITERAL
    :  'X\'' (~'\'')* '\''
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

DOUBLE_VALUE
    : DIGIT+ ('.' DIGIT*)? EXPONENT
    | '.' DIGIT+ EXPONENT
    ;

IDENTIFIER
    : (LETTER | '_') (LETTER | DIGIT | '_')*
    ;

DIGIT_IDENTIFIER
    : DIGIT (LETTER | DIGIT | '_' | '@' | ':')+
    ;

QUOTED_IDENTIFIER
    : '"' ( ~'"' | '""' )* '"'
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

TIME_WITH_TIME_ZONE
    : 'TIME' WS 'WITH' WS 'TIME' WS 'ZONE'
    ;

TIMESTAMP_WITH_TIME_ZONE
    : 'TIMESTAMP' WS 'WITH' WS 'TIME' WS 'ZONE'
    ;

DOUBLE_PRECISION
    : 'DOUBLE' WS 'PRECISION'
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Z]
    ;

SIMPLE_COMMENT
    : '--' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' .*? '*/' -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;
