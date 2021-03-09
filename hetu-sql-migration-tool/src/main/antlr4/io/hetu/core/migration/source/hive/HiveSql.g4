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

grammar HiveSql;

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
        (COMMENT comment=string)? (LOCATION location=string)?
        (WITH DBPROPERTIES properties)?                                             #createSchema
    | DROP (SCHEMA | DATABASE) (IF EXISTS)? qualifiedName (CASCADE | RESTRICT)?     #dropSchema
    | ALTER (SCHEMA | DATABASE) qualifiedName
        (SET DBPROPERTIES properties
          | SET OWNER (USER | ROLE) identifier
          | SET LOCATION string
          | SET MANAGEDLOCATION string)                                             #alterSchema
    | DESCRIBE (SCHEMA | DATABASE) EXTENDED? qualifiedName                          #describeSchema
    | CREATE TEMPORARY? TRANSACTIONAL? TABLE (IF NOT EXISTS)? qualifiedName columnAliases?
        (COMMENT string)?
        (STORED_AS stored_as=identifier)?
        (LOCATION location=string)?
        (TBLPROPERTIES properties)?
        AS query                                                       #createTableAsSelect
    | CREATE TEMPORARY? EXTERNAL? TRANSACTIONAL? TABLE (IF NOT EXISTS)? qualifiedName
        '(' tableElement (',' tableElement)* (',' constraintSpecification)?')'
        (COMMENT string)?
        (PARTITIONED BY '('partitionedBy')')?
        (CLUSTERED BY '('clusteredBy')'
          (SORTED BY '(' sortedBy ')')?
          (INTO bucketcount=expression BUCKETS)
        )?
        (SKEWED BY columnAliases ON expression (',' expression)* (STORED_AS DIRECTORIES)?)?
        ((ROW FORMAT rowFormat)? (STORED_AS stored_as=identifier)? | (STORED BY storedBy=expression (WITH SERDEPROPERTIES serdeProperties=properties)?))
        (LOCATION location=string)?
        (TBLPROPERTIES tableProperties=properties)?                                 #createTable
    | CREATE TEMPORARY? EXTERNAL? TABLE (IF NOT EXISTS)? tableName=qualifiedName
        LIKE likeTableName=qualifiedName (LOCATION location=string)?   #createTableLike
    | DROP TABLE (IF EXISTS)? qualifiedName PURGE?                     #dropTable
    | INSERT INTO (TABLE)? qualifiedName
        (PARTITION '('insertPartition')')? columnAliases? query        #insertInto
    | INSERT OVERWRITE TABLE qualifiedName
        (PARTITION '('insertPartition')' (IF NOT EXISTS)? )?  query    #insertOverwrite
    | INSERT OVERWRITE LOCAL? DIRECTORY identifier
        (ROW FORMAT rowFormat)?
        (STORED_AS stored_as=identifier)?
        query                                                          #insertFilesystem
    | UPDATE qualifiedName SET assignmentList
        (WHERE booleanExpression)?                                     #updateTable
    | DELETE FROM qualifiedName (WHERE booleanExpression)?             #delete
    | ALTER TABLE from=qualifiedName RENAME TO to=qualifiedName        #renameTable
    | ALTER TABLE qualifiedName SET TBLPROPERTIES properties           #commentTable
    | ALTER TABLE tableName=qualifiedName (PARTITION partition=partitionSpec)?
        (ADD | REPLACE) COLUMNS '('tableElement (',' tableElement)*')' (CASCADE | RESTRICT)?                                                    #addReplaceColumn
    | ALTER TABLE qualifiedName (PARTITION partitionSpec)? (SET SERDE string (WITH SERDEPROPERTIES properties)? | SET SERDEPROPERTIES properties)  #alterTableSerde
    | ALTER TABLE qualifiedName (PARTITION partitionSpec)? UNSET SERDEPROPERTIES '(' qualifiedName (',' qualifiedName)* ')'                     #alterRemoveSerde
    | ALTER TABLE qualifiedName CLUSTERED BY columnAliases (SORTED BY columnAliases)? INTO expression BUCKETS                                   #alterTableStorage
    | ALTER TABLE qualifiedName SKEWED BY columnAliases ON  expression (',' expression)* (STORED_AS DIRECTORIES)?                               #alterTableSkewed
    | ALTER TABLE qualifiedName NOT SKEWED                                                                                                      #alterTableNotSkewed
    | ALTER TABLE qualifiedName NOT STORED_AS DIRECTORIES                                                                                       #alterTableNotAsDirectories
    | ALTER TABLE qualifiedName SET SKEWED LOCATION properties                                                                                  #alterTableSetSkewedLocation
    | ALTER TABLE qualifiedName ADD CONSTRAINT identifier
        (PRIMARY KEY columnAliases DISABLE NOVALIDATE
          | FOREIGN KEY columnAliases REFERENCES qualifiedName columnAliases DISABLE NOVALIDATE RELY
          | UNIQUE columnAliases DISABLE NOVALIDATE)                                                                                            #alterTableAddConstraint
    | ALTER TABLE qualifiedName CHANGE COLUMN identifier identifier type
        CONSTRAINT identifier (NOT NULL ENABLE | DEFAULT defaultValue ENABLE | CHECK expression ENABLE)                                         #alterTableChangeConstraint
    | ALTER TABLE qualifiedName DROP CONSTRAINT identifier                                                                                      #alterTableDropConstraint
    | ALTER TABLE qualifiedName ADD (IF NOT EXISTS)? PARTITION partitionSpec (LOCATION string)? (PARTITION partitionSpec (LOCATION string)?)*   #alterTableAddPartition
    | ALTER TABLE qualifiedName PARTITION partitionSpec RENAME TO PARTITION partitionSpec                                                       #alterTableRenamePartition
    | ALTER TABLE qualifiedName EXCHANGE PARTITION partitionSpec WITH TABLE qualifiedName                                                       #alterTableExchangePartition
    | ALTER TABLE qualifiedName RECOVER PARTITIONS                                                                                              #alterTableRecoverPartitions
    | ALTER TABLE qualifiedName DROP (IF EXISTS)? PARTITION partitionSpec (',' PARTITION partitionSpec)? (IGNORE PROTECTION)? PURGE?            #alterTableDropPartition
    | ALTER TABLE qualifiedName (ARCHIVE | UNARCHIVE) PARTITION partitionSpec                                                                   #alterTableArchivePartition
    | ALTER TABLE qualifiedName (PARTITION partitionSpec)? SET FILEFORMAT identifier                                                            #alterTablePartitionFileFormat
    | ALTER TABLE qualifiedName (PARTITION partitionSpec)? SET LOCATION string                                                                  #alterTablePartitionLocation
    | ALTER TABLE qualifiedName TOUCH (PARTITION partitionSpec)?                                                                                #alterTablePartitionTouch
    | ALTER TABLE qualifiedName PARTITION partitionSpec (ENABLE | DISABLE) (NO_DROP CASCADE? | OFFLINE)                                         #alterTablePartitionProtections
    | ALTER TABLE qualifiedName (PARTITION partitionSpec)? COMPACT (MAJOR | MINOR) (AND WAIT)? (WITH OVERWRITE TBLPROPERTIES properties)?       #alterTablePartitionCompact
    | ALTER TABLE qualifiedName PARTITION partitionSpec CONCATENATE                                                                             #alterTablePartitionConcatenate
    | ALTER TABLE qualifiedName PARTITION partitionSpec UPDATE COLUMNS                                                                          #alterTablePartitionUpdateColumns
    | ALTER TABLE qualifiedName (PARTITION partitionSpec)? CHANGE COLUMN? oldName=identifier newName=identifier type
        (COMMENT string)? (FIRST | AFTER columnName=identifier)? (CASCADE | RESTRICT)?                                                          #alterTableChangeColumn
    | CREATE VIEW (IF NOT EXISTS)?  qualifiedName viewColumns?
        (COMMENT string)?
        (TBLPROPERTIES properties)? AS query                           #createView
    | DROP VIEW (IF EXISTS)? qualifiedName                             #dropView
    | ALTER VIEW qualifiedName
        (SET TBLPROPERTIES properties | AS query)                      #alterView
    | SHOW VIEWS ((IN | FROM) qualifiedName)? (LIKE string)?           #showViews
    | CREATE ROLE name=identifier                                      #createRole
    | DROP ROLE name=identifier                                        #dropRole
    | GRANT ROLE?
        roles
        TO principal (',' principal)*
        (WITH ADMIN OPTION)?                                           #grantRoles
    | REVOKE
        (ADMIN OPTION FOR)?
        ROLE?
        roles
        FROM principal (',' principal)*                                #revokeRoles
    | SHOW ROLE GRANT principal                                        #showRoleGrant
    | SHOW PRINCIPALS identifier                                       #showPrincipals
    | SET ROLE (ALL | NONE | role=identifier)                          #setRole
    | GRANT
        privilege (',' privilege)*
        ON qualifiedName TO grantee=principal
        (WITH GRANT OPTION)?                                           #grant
    | REVOKE
        (GRANT OPTION FOR)?
        privilege (',' privilege)*
        ON qualifiedName FROM grantee=principal                        #revoke
    | SHOW GRANT
        principal?
        ON (ALL | TABLE? qualifiedName)                                #showGrants
    | EXPLAIN (EXTENDED | CBO | AST | DEPENDENCY | AUTHORIZATION | LOCKS | VECTORIZATIONANALYZE | ANALYZE)? statement    #explain
    | SHOW CREATE TABLE qualifiedName                                  #showCreateTable
    | SHOW TABLES ((FROM | IN) qualifiedName)?
        (LIKE)? (pattern=string)?                                      #showTables
    | SHOW TABLE EXTENDED ((FROM | IN) qualifiedName)?
        LIKE pattern=identifier (PARTITION properties)?                #showTableExtended
    | SHOW TBLPROPERTIES qualifiedName ('(' identifier ')')?           #showTableProperties
    | TRUNCATE TABLE qualifiedName (PARTITION properties)?             #truncateTable
    | MSCK REPAIR? TABLE qualifiedName
        ((ADD | DROP | SYNC) PARTITIONS)?                              #msckRepairTable
    | SHOW (SCHEMAS | DATABASES)
        (LIKE pattern=string)?                                         #showSchemas
    | SHOW COLUMNS inTable=(FROM | IN) tableName=qualifiedName
        (inDatabase=(FROM | IN) dbName=qualifiedName)?
        (LIKE)? (pattern=string)?                                      #showColumns
    | SHOW CURRENT? ROLES                                              #showRoles
    | (DESCRIBE | DESC) (EXTENDED | FORMATTED)? describeName
        describeTableOption?                                           #describeTable
    | CREATE (TEMPORARY)? FUNCTION qualifiedName
        AS qualifiedName createFunctionOption*                         #createFunction
    | DROP (TEMPORARY)? FUNCTION (IF EXISTS)? qualifiedName            #dropFunction
    | RELOAD (FUNCTIONS | FUNCTION)                                    #reloadFunctions
    | SHOW FUNCTIONS
        (LIKE pattern=string)?                                         #showFunctions
    | SET setProperty?                                                 #setSession
    | RESET                                                            #resetSession
    | CREATE MATERIALIZED VIEW (IF NOT EXISTS)? qualifiedName
        createMaterializedViewOption* AS query                                              #createMaterializedView
    | DROP MATERIALIZED VIEW qualifiedName                                                  #dropMaterializedView
    | ALTER MATERIALIZED VIEW qualifiedName (ENABLE | DISABLE) REWRITE                      #alterMaterializedView
    | SHOW MATERIALIZED VIEWS ((IN | FROM) qualifiedName)? (LIKE pattern=string)?           #showMaterializedViews
    | CREATE CUBE (IF NOT EXISTS)? cubeName=qualifiedName
        ON tableName=qualifiedName
        WITH '(' AGGREGATIONS EQ '(' aggregations ')' ',' GROUP EQ '(' cubeGroup ')' (',' PROPERTIES EQ properties)? ')'   #createCube
    | INSERT INTO CUBE cubeName=qualifiedName WHERE expression     #insertCube
    | INSERT OVERWRITE CUBE cubeName=qualifiedName WHERE expression     #insertOverwriteCube
    | DROP CUBE (IF EXISTS)? cubeName=qualifiedName                    #dropCube
    | SHOW CUBES (FOR tableName=qualifiedName)?                                 #showCubes
    | CREATE INDEX identifier ON TABLE? qualifiedName columnAliases
        AS identifier createIndexOptions*                                                      #createIndex
    | DROP INDEX (IF EXISTS)? identifier ON qualifiedName                                      #dropIndex
    | ALTER INDEX identifier ON qualifiedName (PARTITION properties)? REBUILD                  #alterIndex
    | SHOW (FORMATTED)? (INDEX | INDEXES) ON qualifiedName ((FROM | IN) qualifiedName)?        #showIndex
    | SHOW PARTITIONS qualifiedName (PARTITION properties)?
        (WHERE where=booleanExpression)?
        (ORDER BY sortItem (',' sortItem)*)?
        (LIMIT rows=INTEGER_VALUE)?                                                         #showPartitions
    | (DESCRIBE | DESC) (EXTENDED | FORMATTED)? qualifiedName identifier?
        PARTITION properties                                                                #describePartition
    | (DESCRIBE | DESC) (EXTENDED | FORMATTED)? qualifiedName
        (PARTITION properties)?
        (identifier (('.' identifier) | ('.' '$' identifier '$'))*)?                        #describePartition
    | (DESCRIBE | DESC) FUNCTION EXTENDED? functionName=expression                          #describeFunction
    | CREATE TEMPORARY MACRO identifier '(' (tableElement (',' tableElement)*)? ')' expression    #createMacro
    | DROP TEMPORARY MACRO (IF EXISTS)? identifier                                          #dropMacro
    | SHOW LOCKS (DATABASE | SCHEMA)? qualifiedName (PARTITION properties)? EXTENDED?       #showLocks
    | SHOW CONF identifier                                                                  #showConf
    | SHOW TRANSACTIONS                                                                     #showTransactions
    | SHOW COMPACTIONS                                                                      #showCompactions
    | ABORT TRANSACTIONS INTEGER_VALUE (INTEGER_VALUE)*                                     #abortTransactions
    | LOAD DATA LOCAL? INPATH identifier OVERWRITE? INTO TABLE qualifiedName
        (PARTITION properties)? (INPUTFORMAT identifier SERDE identifier)?                  #loadData
    | MERGE INTO qualifiedName AS T USING (qualifiedName | query) AS S ON booleanExpression
        (WHEN MATCHED (AND booleanExpression)? THEN UPDATE SET property (',' property)*)?
        (WHEN MATCHED (AND booleanExpression)? THEN DELETE)?
        (WHEN NOT MATCHED (AND booleanExpression)? THEN INSERT VALUES expression (',' expression)* )?                #merge
    | EXPORT TABLE qualifiedName (PARTITION properties)? TO string (FOR REPLICATION '(' identifier ')')?             #exportData
    | IMPORT (EXTERNAL? TABLE qualifiedName (PARTITION properties)?)? FROM string (LOCATION string)?                 #importData
    ;

assignmentList
    :  assignmentItem (',' assignmentItem)*
    ;

assignmentItem
    :  qualifiedName EQ expression
    ;

describeName
    : identifier ('.'identifier)?
    ;

describeTableOption
    : (('.' identifier) | ('.' '$' identifier '$'))+
    | identifier (('.' identifier) | ('.' '$' identifier '$'))*
    ;

createFunctionOption
    : USING (JAR | FILE | ARCHIVE) expression (',' (JAR | FILE | ARCHIVE) expression)?
    ;

setProperty
    : identifier ('.' identifier)* EQ expression
    ;

createMaterializedViewOption
    : DISABLE REWRITE
    | COMMENT string
    | PARTITIONED ON columnAliases
    | CLUSTERED ON columnAliases
    | DISTRIBUTE ON columnAliases SORTED ON columnAliases
    | ROW FORMAT expression
    | STORED_AS expression
    | STORED BY expression (WITH SERDEPROPERTIES properties)?
    | LOCATION expression
    | TBLPROPERTIES properties
    ;

createIndexOptions
    : WITH DEFERRED REBUILD
    | IDXPROPERTIES properties
    | IN TABLE qualifiedName
    | (ROW FORMAT expression)? STORED_AS expression
    | STORED BY expression
    | LOCATION string
    | TBLPROPERTIES properties
    | COMMENT string
    ;

viewColumns
    : '(' identifier (COMMENT string)? (',' identifier (COMMENT string)?)* ')'
    ;

query
    :  with? queryNoWith
    ;

with
    : WITH RECURSIVE? namedQuery (',' namedQuery)*
    ;

tableElement
    : columnDefinition
    ;

columnDefinition
    : identifier type columnConstraintSpecification? (COMMENT string)?
    ;

properties
    : '(' property (',' property)* ')'
    ;

partitionSpec
    : '(' identifier (EQ expression)? (',' identifier (EQ expression)?)* ')'
    ;

clusteredBy
    : expression (',' expression)*
    ;

distributeBy
    : expression (',' expression)*
    ;

partitionedBy
    : columnDefinition (',' columnDefinition)*
    ;

rowFormat
    : DELIMITED (FIELDS TERMINATED BY string (ESCAPED BY string)?)?
      (COLLECTION ITEMS TERMINATED BY string)?
      (MAP KEYS TERMINATED BY string)?
      (LINES TERMINATED BY string)?
      (NULL DEFINED AS string)?
    | SERDE string (WITH SERDEPROPERTIES properties)?
    ;

columnConstraintSpecification
    : PRIMARY KEY
    | UNIQUE (DISABLE NOVALIDATE)?
    | NOT NULL
    | DEFAULT defaultValue?
    | CHECK expression? ((ENABLE | DISABLE) NOVALIDATE (RELY | NORELY)?)?
    ;

constraintSpecification
    : PRIMARY KEY columnAliases DISABLE NOVALIDATE (RELY | NORELY)?
    | CONSTRAINT identifier FOREIGN KEY columnAliases REFERENCES qualifiedName columnAliases DISABLE NOVALIDATE
    | CONSTRAINT identifier UNIQUE columnAliases DISABLE NOVALIDATE (RELY | NORELY)?
    | CONSTRAINT identifier CHECK expression? ((ENABLE | DISABLE) NOVALIDATE (RELY | NORELY)?)?
    ;

defaultValue
    : LITERAL
    | CURRENT_USER '(' ')'
    | CURRENT_DATE '(' ')'
    | CURRENT_TIMESTAMP '(' ')'
    | NULL
    ;

insertPartition
    : identifier (EQ expression)? (',' identifier (EQ expression)? )*
    ;

sortedBy
    : sortItem (',' sortItem)*
    ;

property
    : identifier EQ expression
    ;

queryNoWith:
      queryTerm
      (ORDER BY sortItem (',' sortItem)*)?
      (CLUSTER BY clusteredBy)?
      (DISTRIBUTE BY distributeBy)?
      (SORT BY sortedBy)?
      (LIMIT (offset=INTEGER_VALUE',')? rows=INTEGER_VALUE)?
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
    : SELECT setQuantifier? selectItem (',' selectItem)*
      (FROM relation (',' relation)*)?
      (LATERAL VIEW lateralView)*
      (WHERE where=booleanExpression)?
      (GROUP BY groupBy)?
      (HAVING having=booleanExpression)?
    ;

groupBy
    : setQuantifier? groupingElement ((',')? groupingElement)*
    ;

lateralView
    : qualifiedName '(' (expression (',' expression)*)? ')' qualifiedName AS identifier (',' identifier)*
    ;

groupingElement
    : groupingSet                                            #singleGroupingSet
    | ROLLUP '(' (expression (',' expression)*)? ')'         #rollup
    | CUBE '(' (expression (',' expression)*)? ')'           #cube
    | GROUPING SETS '(' groupingSet (',' groupingSet)* ')'   #multipleGroupingSets
    ;

groupingSet
    : '(' (expression (',' expression)*)? ')'
    | expression
    ;

cubeGroup
    : (identifier (',' identifier)*)?
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
    : left=relation (crossJoin | innerJoin | outerJoin | semiJoin)           #joinRelation
    | sampledRelation                                                        #relationDefault
    ;

crossJoin
    : CROSS JOIN right=sampledRelation joinCriteria?
    ;

innerJoin
    : INNER? JOIN right=sampledRelation joinCriteria?
    ;

outerJoin
    : joinType JOIN rightRelation=relation joinCriteria
    ;

semiJoin
    : LEFT SEMI JOIN rightRelation=relation joinCriteria
    ;

joinType
    : LEFT OUTER?
    | RIGHT OUTER?
    | FULL OUTER?
    ;

joinCriteria
    : ON booleanExpression
    ;

sampledRelation
    : aliasedRelation (
        TABLESAMPLE '(' percentage=expression 'PERCENT' ')'
      )?
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
    ;

expression
    : booleanExpression
    ;

booleanExpression
    : valueExpression predicate[$valueExpression.ctx]?             #predicated
    | (NOT | NON) booleanExpression                                #logicalNot
    | left=booleanExpression operator=AND right=booleanExpression  #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression   #logicalBinary
    | booleanExpression
        (EQ valueExpression | IS (NOT | NON)? NULL)                #expressionPredicated
    ;

// workaround for https://github.com/antlr/antlr4/issues/780
predicate[ParserRuleContext value]
    : comparisonOperator right=valueExpression                            #comparison
    | comparisonOperator comparisonQuantifier '(' query ')'               #quantifiedComparison
    | (NOT | NON)? BETWEEN lower=valueExpression AND upper=valueExpression        #between
    | (NOT | NON)? IN '(' expression (',' expression)* ')'                        #inList
    | (NOT | NON)? IN '(' query ')'                                               #inSubquery
    | (NOT | NON)? LIKE pattern=valueExpression                                   #like
    | (NOT | NON)? RLIKE pattern=valueExpression                                  #rlike
    | (NOT | NON)? REGEXP pattern=valueExpression                                 #regexp
    | IS (NOT | NON)? NULL                                                        #nullPredicate
    | IS (NOT | NON)? DISTINCT FROM right=valueExpression                         #distinctFrom
    ;

valueExpression
    : primaryExpression                                                                 #valueExpressionDefault
    | operator=(MINUS | PLUS) valueExpression                                           #arithmeticUnary
    | left=valueExpression operator=(ASTERISK | SLASH | PERCENT) right=valueExpression  #arithmeticBinary
    | left=valueExpression operator=(BITAND | BITOR | BITXOR) right=valueExpression     #arithmeticBit
    | BITNOT valueExpression                                                            #arithmeticBit
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
    | qualifiedName '(' ASTERISK ')' over?                                                #functionCall
    | qualifiedName '(' (setQuantifier? expression (',' expression)*)? ')' over?          #functionCall
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
    | name=CURRENT_DATE ('(' ')')?                                                        #specialDateTimeFunction
    | name=CURRENT_TIMESTAMP ('(' ')')?                                                   #specialDateTimeFunction
    | name=CURRENT_USER '(' ')'                                                           #currentUser
    | SUBSTRING '(' valueExpression FROM valueExpression (FOR valueExpression)? ')'       #substring
    | NORMALIZE '(' valueExpression (',' normalForm)? ')'                                 #normalize
    | EXTRACT '(' identifier FROM valueExpression ')'                                     #extract
    | '(' expression ')'                                                                  #parenthesizedExpression
    | GROUPING '(' (qualifiedName (',' qualifiedName)*)? ')'                              #groupingOperation
    ;

string
    : STRING                                #basicStringLiteral
    ;

inttype
    : INTEGER_VALUE
    ;

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

aggregations
    :  expression (',' expression)*
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
    : ARRAY '<' type '>'
    | MAP '<' type ',' type '>'
    | UNIONTYPE '<' type (',' type)* '>'
    | STRUCT '<' identifier ':' type (',' identifier ':' type)* '>'
    | baseType ('(' typeParameter (',' typeParameter)* ')')?
    ;

typeParameter
    : INTEGER_VALUE | type
    ;

baseType
    : DOUBLE_PRECISION
    | identifier
    ;

whenClause
    : WHEN condition=expression THEN result=expression
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
    : SELECT | DELETE | UPDATE | INSERT | identifier
    ;

qualifiedName
    : identifier ('.' identifier)*
    ;

principal
    : identifier            #unspecifiedPrincipal
    | USER identifier       #userPrincipal
    | ROLE identifier       #rolePrincipal
    | GROUP identifier      #groupPrincipal
    ;

roles
    : identifier (',' identifier)*
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
    : ADD | ADMIN | ALL | ANALYZE | ANY | ARRAY | ASC | AT | AUTHORIZATION | ABORT
    | BERNOULLI
    | CALL | CASCADE | CATALOGS | COLUMN | COLUMNS | COMMENT | COMMIT | COMMITTED | CURRENT | CONF | CHANGE | CHECK | COLLECTION | COMPACTIONS
    | DATA | DATABASE | DATABASES | DATE | DAY | DAYS | DEFINER | DESC | DISTRIBUTED | DEFAULT | DIRECTORY | DIRECTORIES
    | ENABLE | EXCLUDING | EXPLAIN | EXCHANGE | EXPORT
    | FETCH | FILE | FILTER | FIRST | FOLLOWING | FORMAT | FUNCTIONS| FIELDS | FOREIGN
    | GRANT | GRANTED | GRANTS | GRAPHVIZ | GROUP
    | HOUR | HOURS
    | IF | INCLUDING | INPUT | INTERVAL | INVOKER | IO | ISOLATION | IGNORE | IMPORT | INDEX | INDEXES | ITEMS
    | JSON
    | KEY | KEYS
    | LAST | LATERAL | LEVEL | LIMIT | LOCATION | LOGICAL | LOAD | LOCAL | LINES | LOAD | LOCAL | LOCKS
    | MAP | MINUTE | MINUTES | MONTH | MONTHS | MERGE | MSCK
    | NEXT | NFC | NFD | NFKC | NFKD | NO | NONE | NULLIF | NULLS
    | OFFSET | ONLY | OPTION | ORDINALITY | OUTPUT | OVER | OFFLINE
    | PARTITION | PARTITIONS | PATH | POSITION | PRECEDING | PRIVILEGES | PROPERTIES | PROTECTION | PRIMARY | PRINCIPALS
    | RANGE | READ | RENAME | REPEATABLE | REPLACE | RESET | RESTRICT | REVOKE | ROLE | ROLES | ROLLBACK | ROW | ROWS | REBUILD | RECOVER
    | S | SCHEMA | SCHEMAS | SECOND | SECONDS | SECURITY | SERIALIZABLE | SESSION | SET | SETS | SORT
    | SHOW | SOME | START | STATS | STRUCT | SUBSTRING | SYSTEM
    | T | TABLES | TABLESAMPLE  | TRANSACTION | TRANSACTIONS | TRANSACTIONAL | TEXT | TIES | TIME | TIMESTAMP | TO | TRY_CAST | TYPE | TRUNCATE
    | UNBOUNDED | UNCOMMITTED | UNIONTYPE | USE | USER | UNARCHIVE | UNIQUE
    | VALIDATE | VERBOSE | VIEW | VIEWS
    | WORK | WRITE | WAIT
    | YEAR | YEARS
    | ZONE
    ;

ABORT: 'ABORT';
ADD: 'ADD';
ADMIN: 'ADMIN';
AFTER: 'AFTER';
AGGREGATIONS: 'AGGREGATIONS';
ALL: 'ALL';
ALTER: 'ALTER';
ANALYZE: 'ANALYZE';
AND: 'AND';
ANY: 'ANY';
ARCHIVE: 'ARCHIVE';
ARRAY: 'ARRAY';
AS: 'AS';
ASC: 'ASC';
AST: 'AST';
AT: 'AT';
AUTHORIZATION: 'AUTHORIZATION';
BERNOULLI: 'BERNOULLI';
BETWEEN: 'BETWEEN';
PARTITIONED: 'PARTITIONED';
TEMPORARY: 'TEMPORARY';
EXTERNAL: 'EXTERNAL';
CLUSTER: 'CLUSTER';
CLUSTERED: 'CLUSTERED';
SORT: 'SORT';
SORTED: 'SORTED';
DISTRIBUTE: 'DISTRIBUTE';
BUCKETS: 'BUCKETS';
PURGE: 'PURGE';
SKEWED: 'SKEWED';
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
CATALOGS: 'CATALOGS';
CBO: 'CBO';
CHANGE: 'CHANGE';
CHECK: 'CHECK';
COLLECTION: 'COLLECTION';
COLUMN: 'COLUMN';
COLUMNS: 'COLUMNS';
CONCATENATE: 'CONCATENATE';
COMPACTIONS: 'COMPACTIONS';
COMMENT: 'COMMENT';
COMMIT: 'COMMIT';
COMMITTED: 'COMMITTED';
COMPACT: 'COMPACT';
CONF: 'CONF';
CONSTRAINT: 'CONSTRAINT';
CREATE: 'CREATE';
CROSS: 'CROSS';
CUBE: 'CUBE';
CUBES: 'CUBES';
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
DEFAULT: 'DEFAULT';
DEFERRED: 'DEFERRED';
DELIMITED: 'DELIMITED';
DEFINER: 'DEFINER';
DEPENDENCY: 'DEPENDENCY';
DELETE: 'DELETE';
DISABLE: 'DISABLE';
UPDATE: 'UPDATE';
DEFINED: 'DEFINED';
DESC: 'DESC';
DESCRIBE: 'DESCRIBE';
DIRECTORY: 'DIRECTORY';
DIRECTORIES: 'DIRECTORIES';
DISTINCT: 'DISTINCT';
DISTRIBUTED: 'DISTRIBUTED';
DROP: 'DROP';
ELSE: 'ELSE';
ENABLE: 'ENABLE';
END: 'END';
ESCAPE: 'ESCAPE';
ESCAPED: 'ESCAPED';
EXCEPT: 'EXCEPT';
EXCHANGE: 'EXCHANGE';
EXCLUDING: 'EXCLUDING';
EXECUTE: 'EXECUTE';
EXISTS: 'EXISTS';
EXPLAIN: 'EXPLAIN';
EXPORT: 'EXPORT';
EXTRACT: 'EXTRACT';
EXTENDED: 'EXTENDED';
FALSE: 'FALSE';
FETCH: 'FETCH';
FILE: 'FILE';
FILEFORMAT: 'FILEFORMAT';
FILTER: 'FILTER';
FIELDS: 'FIELDS';
FIRST: 'FIRST';
FOLLOWING: 'FOLLOWING';
FOR: 'FOR';
FOREIGN: 'FOREIGN';
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
HAVING: 'HAVING';
HOUR: 'HOUR';
HOURS: 'HOURS';
IDXPROPERTIES: 'IDXPROPERTIES';
IF: 'IF';
IGNORE: 'IGNORE';
IMPORT: 'IMPORT';
IN: 'IN';
INCLUDING: 'INCLUDING';
INDEX: 'INDEX';
INDEXES: 'INDEXES';
INPATH: 'INPATH';
INPUTFORMAT: 'INPUTFORMAT';
INNER: 'INNER';
INPUT: 'INPUT';
INSERT: 'INSERT';
INTERSECT: 'INTERSECT';
INTERVAL: 'INTERVAL';
INTO: 'INTO';
INVOKER: 'INVOKER';
IO: 'IO';
IS: 'IS';
ISOLATION: 'ISOLATION';
ITEMS: 'ITEMS';
JAR: 'JAR';
JSON: 'JSON';
JOIN: 'JOIN';
KEY: 'KEY';
KEYS: 'KEYS';
LAST: 'LAST';
LATERAL: 'LATERAL';
LEFT: 'LEFT';
LEVEL: 'LEVEL';
LIKE: 'LIKE';
LIMIT: 'LIMIT';
LINES: 'LINES';
LITERAL: 'LITERAL';
LOAD: 'LOAD';
LOCAL: 'LOCAL';
LOCALTIME: 'LOCALTIME';
LOCALTIMESTAMP: 'LOCALTIMESTAMP';
LOCKS: 'LOCKS';
LOGICAL: 'LOGICAL';
MACRO: 'MACRO';
MAJOR: 'MAJOR';
MINOR: 'MINOR';
MANAGEDLOCATION: 'MANAGEDLOCATION';
MATERIALIZED: 'MATERIALIZED';
MAP: 'MAP';
MATCHED: 'MATCHED';
MERGE: 'MERGE';
MINUTE: 'MINUTE';
MINUTES: 'MINUTES';
MONTH: 'MONTH';
MONTHS: 'MONTHS';
MSCK: 'MSCK';
NATURAL: 'NATURAL';
NEXT: 'NEXT';
NFC : 'NFC';
NFD : 'NFD';
NFKC : 'NFKC';
NFKD : 'NFKD';
NO: 'NO';
NO_DROP: 'NO_DROP';
NONE: 'NONE';
NORMALIZE: 'NORMALIZE';
NOVALIDATE: 'NOVALIDATE';
NORELY: 'NORELY';
NOT: 'NOT';
NULL: 'NULL';
NULLIF: 'NULLIF';
NULLS: 'NULLS';
OFFLINE: 'OFFLINE';
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
POSITION: 'POSITION';
PRECEDING: 'PRECEDING';
PREPARE: 'PREPARE';
PROTECTION: 'PROTECTION';
PRIMARY: 'PRIMARY';
PRINCIPALS: 'PRINCIPALS';
PRIVILEGES: 'PRIVILEGES';
PROPERTIES: 'PROPERTIES';
S: 'S';
RANGE: 'RANGE';
READ: 'READ';
REBUILD: 'REBUILD';
RECOVER: 'RECOVER';
RELOAD: 'RELOAD';
RELY: 'RELY';
RECURSIVE: 'RECURSIVE';
REFERENCES: 'REFERENCES';
REGEXP: 'REGEXP';
RENAME: 'RENAME';
REPEATABLE: 'REPEATABLE';
REPAIR: 'REPAIR';
REPLACE: 'REPLACE';
REPLICATION: 'REPLICATION';
REWRITE: 'REWRITE';
RESET: 'RESET';
RESTRICT: 'RESTRICT';
REVOKE: 'REVOKE';
RIGHT: 'RIGHT';
RLIKE: 'RLIKE';
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
SEMI: 'SEMI';
SERDEPROPERTIES: 'SERDEPROPERTIES';
SERDE: 'SERDE';
SERIALIZABLE: 'SERIALIZABLE';
SESSION: 'SESSION';
SET: 'SET';
SETS: 'SETS';
SHOW: 'SHOW';
SOME: 'SOME';
START: 'START';
STATS: 'STATS';
STRUCT: 'STRUCT';
SUBSTRING: 'SUBSTRING';
SYNC: 'SYNC';
SYSTEM: 'SYSTEM';
T: 'T';
TABLE: 'TABLE';
TABLES: 'TABLES';
TABLESAMPLE: 'TABLESAMPLE';
TERMINATED: 'TERMINATED';
TEXT: 'TEXT';
THEN: 'THEN';
TIES: 'TIES';
TIME: 'TIME';
TIMESTAMP: 'TIMESTAMP';
TO: 'TO';
TOUCH: 'TOUCH';
TRANSACTION: 'TRANSACTION';
TRANSACTIONS: 'TRANSACTIONS';
TRANSACTIONAL: 'TRANSACTIONAL';
TRUE: 'TRUE';
TRUNCATE: 'TRUNCATE';
TRY_CAST: 'TRY_CAST';
TYPE: 'TYPE';
UESCAPE: 'UESCAPE';
UNARCHIVE: 'UNARCHIVE';
UNBOUNDED: 'UNBOUNDED';
UNCOMMITTED: 'UNCOMMITTED';
UNIQUE: 'UNIQUE';
UNION: 'UNION';
UNIONTYPE: 'UNIONTYPE ';
UNNEST: 'UNNEST';
UNSET: 'UNSET';
USE: 'USE';
USER: 'USER';
USING: 'USING';
VALIDATE: 'VALIDATE';
VALUES: 'VALUES';
VERBOSE: 'VERBOSE';
VIEW: 'VIEW';
VIEWS: 'VIEWS';
VECTORIZATIONANALYZE: 'VECTORIZATIONANALYZE';
WAIT: 'WAIT';
WHEN: 'WHEN';
WHERE: 'WHERE';
WITH: 'WITH';
WORK: 'WORK';
WRITE: 'WRITE';
YEAR: 'YEAR';
YEARS: 'YEARS';
ZONE: 'ZONE';

EQ  : '=' | '==';
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
NON: '!';
BITAND: '&';
BITOR: '|';
BITXOR: '^';
BITNOT: '~';

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
    ;

DOUBLE_VALUE
    : DIGIT+ ('.' DIGIT*)? EXPONENT
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
