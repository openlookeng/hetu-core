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
package io.hetu.core.plugin.oracle;

import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.hetu.core.plugin.oracle.config.RoundingMode;
import io.hetu.core.plugin.oracle.config.UnsupportedTypeHandling;
import io.hetu.core.plugin.oracle.optimization.OracleQueryGenerator;
import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.LongWriteFunction;
import io.prestosql.plugin.jdbc.SliceWriteFunction;
import io.prestosql.plugin.jdbc.StatsCollecting;
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.plugin.jdbc.optimization.JdbcPushDownModule;
import io.prestosql.plugin.jdbc.optimization.JdbcPushDownParameter;
import io.prestosql.plugin.jdbc.optimization.JdbcQueryGeneratorResult;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.SuppressFBWarnings;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.relation.RowExpressionService;
import io.prestosql.spi.sql.QueryGenerator;
import io.prestosql.spi.type.AbstractType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DateTimeEncoding;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.VarcharType;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.realColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.timestampColumnMappingUsingSqlTimestamp;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.timestampWriteFunctionUsingSqlTimestamp;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varbinaryColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.CharType.createCharType;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.Decimals.encodeShortScaledValue;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static java.lang.Byte.toUnsignedInt;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * Implementation of OracleClient. It describes table, schemas and columns behaviours.
 * It allows to change the QueryBuilder to a custom one as well.
 */
public class OracleClient
        extends BaseJdbcClient
{
    private static final Logger log = Logger.get(OracleClient.class);

    private static final String NUMBER_DATA_TYPE_NAME = "NUMBER";

    private static final String NCLOB_STRING = "NCLOB";

    private static final String NVARCHAR2_STRING = "NVARCHAR2";

    private static final String OPENINGBRACKET_STRING = "(";

    private static final String CLOSINGBRACKET_STRING = ")";

    private static final int INDEX_OF_YEAR_ZERO = 0;

    private static final int INDEX_OF_YEAR_ONE = 1;

    private static final int INDEX_OF_MONTH = 2;

    private static final int TINY_INT_COLUMN_SIZE = 3;

    private static final int INDEX_OF_DAY_OF_MONTH = 3;

    private static final int INDEX_OF_HOUR = 4;

    private static final int SMALL_INT_COLUMN_SIZE = 5;

    private static final int INDEX_OF_MINUTE = 5;

    private static final int INDEX_OF_SECOND = 6;

    private static final int INDEX_OF_NANO_SECOND = 7;

    private static final int DB_BYTE_INDEX_EIGHT = 8;

    private static final int DB_BYTE_INDEX_NINE = 9;

    private static final int INTEGER_COLUMN_SIZE = 10;

    private static final int NUMBER_OF_BIT_SHIFTS_SIXTEEN = 16;

    private static final int BIG_INT_COLUMN_SIZE = 19;

    private static final int NUMBER_OF_BIT_SHIFT_TWENTY_FOUR = 24;

    private static final int TEMPORARY_TABLE_NAME_MAX_LENGTH = 30;

    private static final int INVALID_DECIMAL_DIGITS = -127;

    private static final int NUMBER_OF_YEAR_HUNDRED = 100;

    private static final int MAX_NVARCHAR2_LENGTH = 4000;

    private final int numberDefaultScale;

    private final RoundingMode roundingMode;

    private final UnsupportedTypeHandling unsupportedTypeHandling;

    /**
     * If disabled, do not accept sub-query push down.
     */
    private final JdbcPushDownModule pushDownModule;

    /**
     * enable to user oracle synonyms
     */
    private final boolean synonymsEnabled;

    /**
     * Create Oracle client using the configurations.
     *
     * @param config config
     * @param oracleConfig oracleConfig
     * @param connectionFactory connectionFactory
     */
    @Inject
    public OracleClient(BaseJdbcConfig config, OracleConfig oracleConfig,
            @StatsCollecting ConnectionFactory connectionFactory)
    {
        // the empty "" is to not use a quote to create queries
        super(config, "\"", connectionFactory);
        this.pushDownModule = config.getPushDownModule();
        this.numberDefaultScale = oracleConfig.getNumberDefaultScale();
        this.roundingMode = requireNonNull(oracleConfig.getRoundingMode(), "oracle rounding mode cannot be null");
        this.unsupportedTypeHandling = requireNonNull(oracleConfig.getUnsupportedTypeHandling(),
                "oracle unsupported type handling cannot be null");
        this.synonymsEnabled = oracleConfig.isSynonymsEnabled();
    }

    private static ColumnMapping charColumnMapping(CharType charType)
    {
        requireNonNull(charType, "charType is null");
        return ColumnMapping.sliceMapping(charType, (resultSet, columnIndex) -> utf8Slice(
                CharMatcher.is(' ').trimTrailingFrom(resultSet.getString(columnIndex))), charWriteFunction(charType.getLength()));
    }

    private static SliceWriteFunction charWriteFunction(int charTypeLength)
    {
        return (statement, index, value) -> {
            // https://docs.oracle.com/cd/E18283_01/appdev.112/e13995/oracle/jdbc/OraclePreparedStatement.html#setFixedCHAR_int__java_lang_String_
            String valueString = value.toStringUtf8();
            statement.setString(index, appendSpace(charTypeLength - valueString.length(), valueString));
        };
    }

    private static String appendSpace(int spaceLen, String head)
    {
        StringBuilder builder = new StringBuilder(head);
        for (int i = 0; i < spaceLen; i++) {
            builder.append(" ");
        }
        return builder.toString();
    }

    private static LocalDateTime extractLocalDateTime(byte[] bytes)
    {
        int year = ((toUnsignedInt(bytes[INDEX_OF_YEAR_ZERO]) - NUMBER_OF_YEAR_HUNDRED) * NUMBER_OF_YEAR_HUNDRED) + (
                toUnsignedInt(bytes[INDEX_OF_YEAR_ONE]) - NUMBER_OF_YEAR_HUNDRED);
        int month = bytes[INDEX_OF_MONTH];
        int dayOfMonth = bytes[INDEX_OF_DAY_OF_MONTH];
        int hour = bytes[INDEX_OF_HOUR] - 1;
        int minute = bytes[INDEX_OF_MINUTE] - 1;
        int second = bytes[INDEX_OF_SECOND] - 1;
        int nanoOfSecond = toUnsignedInt(bytes[INDEX_OF_NANO_SECOND]) << NUMBER_OF_BIT_SHIFT_TWENTY_FOUR
                | toUnsignedInt(bytes[DB_BYTE_INDEX_EIGHT]) << NUMBER_OF_BIT_SHIFTS_SIXTEEN
                | toUnsignedInt(bytes[DB_BYTE_INDEX_NINE]) << DB_BYTE_INDEX_EIGHT | toUnsignedInt(
                bytes[INTEGER_COLUMN_SIZE]);
        return LocalDateTime.of(year, month, dayOfMonth, hour, minute, second, nanoOfSecond);
    }

    /**
     * timestamp with time zone
     *
     * @param connectorSession connectorSession
     * @return LongWriteFunction
     * @deprecated This method uses {@link java.sql.Timestamp} and the class cannot
     * represent date-time value when JVM zone had
     * forward offset change (a 'gap'). This includes regular DST changes (e.g. Europe/Warsaw)
     * and one-time policy changes
     * (Asia/Kathmandu's shift by 15 minutes on January 1, 1986, 00:00:00). If driver only
     * supports {@link LocalDateTime}, use
     */
    @Deprecated
    public static LongWriteFunction timestampWithTimeZoneWriteFunctionUsingSqlTimestamp(
            ConnectorSession connectorSession)
    {
        if (connectorSession.isLegacyTimestamp()) {
            ZoneId sessionZone = ZoneId.of(connectorSession.getTimeZoneKey().getId());
            return (statement, index, value) -> setTimestampWithTimeZoneLegacy(statement, index, value, sessionZone);
        }
        return (statement, index, value) -> setTimestampWithTimeZone(statement, index, value);
    }

    private static void setTimestampWithTimeZoneLegacy(PreparedStatement statement, int index, long value,
            ZoneId sessionZone)
    {
        try {
            statement.setTimestamp(index, new Timestamp(DateTimeEncoding.unpackMillisUtc(
                    fromHetuLegacyTimestamp(value, sessionZone).atZone(sessionZone).toInstant().toEpochMilli())));
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, "Hetu Oracle connector failed to set Timestamp With Time Zone Legacy");
        }
    }

    private static void setTimestampWithTimeZone(PreparedStatement statement, int index, long value)
    {
        try {
            statement.setTimestamp(index, new Timestamp(
                    DateTimeEncoding.unpackMillisUtc(fromHetuTimestamp(value).atZone(UTC).toInstant().toEpochMilli())));
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, "Hetu Oracle connector failed to set Timestamp With Time Zone");
        }
    }

    private static LocalDateTime fromHetuLegacyTimestamp(long value, ZoneId sessionZone)
    {
        return Instant.ofEpochMilli(value).atZone(sessionZone).toLocalDateTime();
    }

    private static LocalDateTime fromHetuTimestamp(long value)
    {
        return Instant.ofEpochMilli(value).atZone(UTC).toLocalDateTime();
    }

    @Override
    protected Collection<String> listSchemas(Connection connection)
    {
        try (ResultSet resultSet = connection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString(1).toLowerCase(Locale.ENGLISH);
                schemaNames.add(schemaName);
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw new RuntimeException("Hetu Oracle connector failed to list schemas");
        }
    }

    private String[] getTableTypes()
    {
        if (synonymsEnabled) {
            return new String[] {"TABLE", "SYNONYM", "VIEW"};
        }
        return new String[] {"TABLE", "VIEW"};
    }

    @Override
    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        return connection.getMetaData()
                .getTables(connection.getCatalog(), schemaName.orElse(null), tableName.orElse(null),
                        getTableTypes());
    }

    @Nullable
    @Override
    public Optional<JdbcTableHandle> getTableHandle(JdbcIdentity identity, SchemaTableName schemaTableName)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            DatabaseMetaData metadata = connection.getMetaData();
            String jdbcSchemaName = schemaTableName.getSchemaName();
            String jdbcTableName = schemaTableName.getTableName();
            if (metadata.storesUpperCaseIdentifiers()) {
                jdbcSchemaName = jdbcSchemaName.toUpperCase(Locale.ENGLISH);
                jdbcTableName = jdbcTableName.toUpperCase(Locale.ENGLISH);
            }
            try (ResultSet resultSet = getTables(connection, Optional.of(jdbcSchemaName), Optional.of(jdbcTableName))) {
                List<JdbcTableHandle> tableHandles = new ArrayList<>(1);
                while (resultSet.next()) {
                    if (jdbcTableName.equals(resultSet.getString(Constants.TABLE_NAME).toUpperCase(Locale.ENGLISH))) {
                        tableHandles.add(new JdbcTableHandle(schemaTableName, resultSet.getString("TABLE_CAT"),
                                resultSet.getString("TABLE_SCHEM"), resultSet.getString(Constants.TABLE_NAME)));
                    }
                }
                if (tableHandles.isEmpty()) {
                    return Optional.empty();
                }
                if (tableHandles.size() > 1) {
                    throw new PrestoException(NOT_SUPPORTED, "Multiple tables matched: " + schemaTableName);
                }
                return Optional.of(getOnlyElement(tableHandles));
            }
            catch (SQLException e) {
                return Optional.empty();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Hetu oracle connector failed to get table handle");
        }
    }

    @SuppressFBWarnings("SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING")
    @Override
    public Map<String, ColumnHandle> getColumns(ConnectorSession session, String sql, Map<String, Type> types)
    {
        try (Connection connection = connectionFactory.openConnection(JdbcIdentity.from(session));
                PreparedStatement statement = connection.prepareStatement(sql)) {
            ResultSetMetaData metadata = statement.getMetaData();
            ImmutableMap.Builder<String, ColumnHandle> builder = new ImmutableMap.Builder<>();

            for (int i = 1; i <= metadata.getColumnCount(); i++) {
                String columnName = metadata.getColumnLabel(i);
                String typeName = metadata.getColumnTypeName(i);
                int precision = metadata.getPrecision(i);
                int dataType = metadata.getColumnType(i);
                int scale = metadata.getScale(i);
                boolean isNullAble = metadata.isNullable(i) != ResultSetMetaData.columnNoNulls;

                // Oracle JDBC returns decimal(0, -127) for the output of SQL functions like avg, sum, or count
                // decimal(0, -127) is an invalid precision and scale combination which cannot be used to
                // create Presto type. The following if block uses the Presto type for that specific column
                // extracted from the logical plan during pre-processing.
                // For example, if decimal(0, -127) is returned for the sub-query
                // ` SELECT avg(age) as x FROM USER`,
                // but in Presto logical-plan the column x's type is integer, the following code will define
                // the type of that column as integer.
                if ((dataType == Types.DECIMAL || NUMBER_DATA_TYPE_NAME.equalsIgnoreCase(typeName)) && (precision == 0
                        || scale < 0)) {
                    // Covert Oracle NUMBER(scale) type to Presto types
                    String loweredColumnName = columnName.toLowerCase(ENGLISH);
                    Type hetuType = types.get(loweredColumnName);
                    if (hetuType instanceof AbstractType) {
                        TypeSignature signature = hetuType.getTypeSignature();
                        typeName = signature.getBase().toUpperCase(ENGLISH);
                        dataType = JDBCType.valueOf(typeName).getVendorTypeNumber();
                        if (hetuType instanceof DecimalType && this.roundingMode != RoundingMode.UNNECESSARY) {
                            precision = ((DecimalType) hetuType).getPrecision();
                            scale = ((DecimalType) hetuType).getScale();
                        }
                    }
                }

                JdbcTypeHandle typeHandle = new JdbcTypeHandle(dataType, Optional.ofNullable(typeName), precision,
                        scale, Optional.empty());
                Optional<ColumnMapping> columnMapping;
                try {
                    columnMapping = toPrestoType(session, connection, typeHandle);
                }
                catch (UnsupportedOperationException ex) {
                    // User configured to fail the query if the data type is not supported
                    return Collections.emptyMap();
                }
                // skip unsupported column types
                if (columnMapping.isPresent()) {
                    Type type = columnMapping.get().getType();
                    JdbcColumnHandle handle = new JdbcColumnHandle(columnName, typeHandle, type, isNullAble);
                    builder.put(columnName.toLowerCase(ENGLISH), handle);
                }
            }

            return builder.build();
        }
        catch (SQLException | PrestoException e) {
            // No need to raise an error.
            // This method is used inside applySubQuery method to extract the column types from a sub-query.
            // Returning empty map will indicate that something wrong and let the Presto to execute the query as usual.
            return Collections.emptyMap();
        }
    }

    @Override
    public List<SchemaTableName> getTableNames(JdbcIdentity identity, Optional<String> schema)
    {
        Optional<String> schemas = Optional.empty();
        try (Connection connection = connectionFactory.openConnection(identity)) {
            DatabaseMetaData metadata = connection.getMetaData();
            if (metadata.storesUpperCaseIdentifiers() && schema.isPresent()) {
                schemas = Optional.of(schema.get().toUpperCase(Locale.ENGLISH));
            }
            try (ResultSet resultSet = getTables(connection, schemas, Optional.empty())) {
                ImmutableList.Builder<SchemaTableName> list = ImmutableList.builder();
                while (resultSet.next()) {
                    String tableSchema = getTableSchemaName(resultSet);
                    String tableName = resultSet.getString(Constants.TABLE_NAME);
                    list.add(new SchemaTableName(tableSchema.toLowerCase(ENGLISH), tableName.toLowerCase(ENGLISH)));
                }
                return list.build();
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, "Hetu oracle connector failed to get table names");
        }
    }

    @Override
    public void renameTable(JdbcIdentity identity, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        renameTable(identity, handle.getCatalogName(), handle.getSchemaName(), handle.getTableName(), newTableName);
    }

    /**
     * Rename the table to the given name.
     *
     * @param identity identity
     * @param catalogName catalogName
     * @param schemaName schemaName
     * @param tableName tableName
     * @param newTable newTable
     */
    protected void renameTable(JdbcIdentity identity, String catalogName, String schemaName, String tableName,
            SchemaTableName newTable)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            String newSchemaName = newTable.getSchemaName();
            String newTableName = newTable.getTableName();
            if (connection.getMetaData().storesUpperCaseIdentifiers()) {
                newSchemaName = newSchemaName.toUpperCase(ENGLISH);
                newTableName = newTableName.toUpperCase(ENGLISH);
            }
            String sql = format("ALTER TABLE %s RENAME TO %s", quoted(catalogName, schemaName, tableName),
                    quoted(catalogName, newSchemaName.equals(schemaName) ? "" : newSchemaName, newTableName));

            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, "Hetu Oracle connector failed to rename table");
        }
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, Connection connection,
            JdbcTypeHandle typeHandle)
    {
        // https://docs.starburstdata.com/latest/connector/oracle.html
        Optional<ColumnMapping> columnMapping = Optional.empty();

        int columnSize = typeHandle.getColumnSize();
        String jdbcTypeName = typeHandle.getJdbcTypeName().get().toUpperCase(ENGLISH);
        switch (typeHandle.getJdbcType()) {
            case OracleTypes.BINARY_FLOAT:
                columnMapping = Optional.of(realColumnMapping());
                break;

            case OracleTypes.BINARY_DOUBLE:
                columnMapping = Optional.of(doubleColumnMapping());
                break;

            case Types.CHAR:
            case Types.NCHAR:
            case OracleTypes.CHAR_OR_NCHAR:
                int charLength = min(columnSize, CharType.MAX_LENGTH);
                columnMapping = Optional.of(charColumnMapping(createCharType(charLength)));
                break;

            case OracleTypes.CLOB_OR_NCLOB:
                columnMapping = Optional.of(varcharColumnMapping(createUnboundedVarcharType()));
                break;

            case OracleTypes.BLOB:
            case OracleTypes.RAW:
            case OracleTypes.LONG_RAW:
                columnMapping = Optional.of(varbinaryColumnMapping());
                break;

            case OracleTypes.TIMESTAMP:
                columnMapping = Optional.of(timestampColumnMappingUsingSqlTimestamp(session));
                break;

            // the following two data type is not supported because of oracle.sql.TIMESTAMPTZ
            // OracleTypes.TIMESTAMP6_WITH_TIMEZONE:
            // OracleTypes.TIMESTAMP_STRING:
            case OracleTypes.TIMESTAMP_WITH_TIMEZONE_OR_NCLOB_OR_NVARCHAR2:
                if (NCLOB_STRING.equals(jdbcTypeName)) {
                    columnMapping = Optional.of(varcharColumnMapping(createUnboundedVarcharType()));
                }
                else if (NVARCHAR2_STRING.equals(jdbcTypeName)) {
                    columnMapping = Optional.of(
                            varcharColumnMapping(createVarcharType(min(columnSize, MAX_NVARCHAR2_LENGTH))));
                }
                break;

            case Types.NUMERIC:
            case Types.DECIMAL:
                int decimalDigits = typeHandle.getDecimalDigits();
                int precision = columnSize + max(-decimalDigits,
                        0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).

                if (decimalDigits == 0) { // integer
                    if (columnSize == TINY_INT_COLUMN_SIZE) {
                        columnMapping = Optional.of(tinyintColumnMapping());
                    }
                    else if (columnSize == SMALL_INT_COLUMN_SIZE) {
                        columnMapping = Optional.of(smallintColumnMapping());
                    }
                    else if (columnSize == INTEGER_COLUMN_SIZE) {
                        columnMapping = Optional.of(integerColumnMapping());
                    }
                    else if (columnSize == BIG_INT_COLUMN_SIZE) {
                        columnMapping = Optional.of(bigintColumnMapping());
                    }
                }
                if (columnSize == 0 && decimalDigits == INVALID_DECIMAL_DIGITS && this.numberDefaultScale >= 0) {
                    columnMapping = Optional.of(
                            decimalColumnMapping(createDecimalType(Decimals.MAX_PRECISION, this.numberDefaultScale)));
                }
                else if (decimalDigits <= columnSize && columnSize <= Decimals.MAX_PRECISION
                        && precision <= Decimals.MAX_PRECISION) {
                    columnMapping = Optional.of(
                            decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0))));
                }
                else if (columnSize > Decimals.MAX_PRECISION && decimalDigits == INVALID_DECIMAL_DIGITS
                        && typeHandle.getJdbcType() == Types.NUMERIC) {
                    columnMapping = Optional.of(doubleColumnMapping());
                }
                break;
            case OracleTypes.LONG:
                columnMapping = Optional.empty();
                break;
            default:
                columnMapping = super.toPrestoType(session, connection, typeHandle);
        }

        if (!columnMapping.isPresent()) {
            if (this.unsupportedTypeHandling == UnsupportedTypeHandling.FAIL) {
                throw new UnsupportedOperationException(
                        "Hetu does not support the Oracle type " + typeHandle.getJdbcTypeName().orElse("")
                                + OPENINGBRACKET_STRING + typeHandle.getColumnSize() + ", " + typeHandle.getDecimalDigits()
                                + CLOSINGBRACKET_STRING);
            }
            else if (this.unsupportedTypeHandling == UnsupportedTypeHandling.CONVERT_TO_VARCHAR) {
                columnMapping = Optional.of(varcharColumnMapping(createUnboundedVarcharType()));
            }
        }
        return columnMapping;
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type instanceof VarcharType) {
            VarcharType varcharType = (VarcharType) type;
            String dataType;
            if (varcharType.isUnbounded() || varcharType.getBoundedLength() > MAX_NVARCHAR2_LENGTH) {
                dataType = "NCLOB";
            }
            else {
                dataType = "NVARCHAR2(" + varcharType.getBoundedLength() + CLOSINGBRACKET_STRING;
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }
        else if (type instanceof CharType) {
            return WriteMapping.sliceMapping("char(" + ((CharType) type).getLength() + CLOSINGBRACKET_STRING,
                    charWriteFunction(((CharType) type).getLength()));
        }
        else if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.sliceMapping(dataType, longDecimalWriteFunction(decimalType));
        }
        else if (BOOLEAN.equals(type)) {
            return WriteMapping.booleanMapping("NUMBER(1)", booleanWriteFunction());
        }
        else if (INTEGER.equals(type)) {
            return WriteMapping.longMapping("NUMBER(10)", integerWriteFunction());
        }
        else if (SMALLINT.equals(type)) {
            return WriteMapping.longMapping("NUMBER(5)", smallintWriteFunction());
        }
        else if (BIGINT.equals(type)) {
            return WriteMapping.longMapping("NUMBER(19)", bigintWriteFunction());
        }
        else if (TINYINT.equals(type)) {
            return WriteMapping.longMapping("NUMBER(3)", tinyintWriteFunction());
        }
        else if (REAL.equals(type)) {
            return WriteMapping.longMapping("BINARY_FLOAT", realWriteFunction());
        }
        else if (DOUBLE.equals(type)) {
            return WriteMapping.doubleMapping("BINARY_DOUBLE", doubleWriteFunction());
        }
        else if (VARBINARY.equals(type)) {
            return WriteMapping.sliceMapping("BLOB", varbinaryWriteFunction());
        }
        else if (TIMESTAMP.equals(type)) {
            return WriteMapping.longMapping("TIMESTAMP", timestampWriteFunctionUsingSqlTimestamp(session));
        }
        else if (TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
            return WriteMapping.longMapping("TIMESTAMP(3) WITH TIME ZONE",
                    timestampWithTimeZoneWriteFunctionUsingSqlTimestamp(session));
        }
        else if (DATE.equals(type)) {
            return WriteMapping.longMapping("DATE", timestampWriteFunctionUsingSqlTimestamp(session));
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
        }
    }

    @Override
    public Optional<QueryGenerator<JdbcQueryGeneratorResult>> getQueryGenerator(RowExpressionService rowExpressionService)
    {
        JdbcPushDownParameter pushDownParameter = new JdbcPushDownParameter(getIdentifierQuote(), this.caseInsensitiveNameMatching, pushDownModule);
        return Optional.of(new OracleQueryGenerator(rowExpressionService, pushDownParameter));
    }

    private ColumnMapping decimalColumnMapping(DecimalType decimalType)
    {
        // JDBC driver can return BigDecimal with lower scale than column's scale when there are trailing zeroes
        int scale = decimalType.getScale();
        if (decimalType.isShort()) {
            return ColumnMapping.longMapping(decimalType,
                    (resultSet, columnIndex) -> encodeShortScaledValue(resultSet.getBigDecimal(columnIndex), scale),
                    shortDecimalWriteFunction(decimalType));
        }
        return ColumnMapping.sliceMapping(decimalType,
                (resultSet, columnIndex) -> encodeScaledValue(resultSet.getBigDecimal(columnIndex), scale),
                longDecimalWriteFunction(decimalType));
    }

    private Slice encodeScaledValue(BigDecimal value, int scale)
    {
        return Decimals.encodeScaledValue(value.setScale(scale, this.roundingMode.getValue()));
    }

    /**
     * generateTemporaryTableName
     *
     * @return String
     */
    protected String generateTemporaryTableName()
    {
        return super.generateTemporaryTableName().substring(0, TEMPORARY_TABLE_NAME_MAX_LENGTH);
    }
}
