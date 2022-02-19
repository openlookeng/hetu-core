/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.cli;

import com.google.common.collect.Lists;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.ReloadCube;
import org.jline.terminal.Terminal;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ReloadCubeConsole
{
    private Console console;
    private String newQuery;
    private String objectName;
    private String schemaName;
    private String catalogName;

    public String getNewQuery()
    {
        return this.newQuery;
    }

    public ReloadCubeConsole(Console console)
    {
        this.console = console;
    }

    public boolean reload(String query, QueryRunner queryRunner, ClientOptions.OutputFormat outputFormat, Runnable schemaChanged, boolean usePager, boolean showProgress, Terminal terminal, PrintStream out, PrintStream errorChannel) throws UnsupportedEncodingException
    {
        SqlParser parser = new SqlParser();
        ReloadCube reloadCube = (ReloadCube) parser.createStatement(query, new ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE));
        if (!checkCubeName(queryRunner, reloadCube, reloadCube.getCubeName())) {
            return false;
        }
        String cubeTableName = this.catalogName + "." + this.schemaName + "." + this.objectName;
        final Charset charset = StandardCharsets.UTF_8;
        ByteArrayOutputStream stringOutputStream = new ByteArrayOutputStream();
        String showCreateCubeQuery = "SHOW CREATE CUBE " + cubeTableName.toString();
        if (!console.runQuery(queryRunner, showCreateCubeQuery, ClientOptions.OutputFormat.CSV, schemaChanged, false, showProgress, terminal, new PrintStream(stringOutputStream, true, charset.name()), errorChannel)) {
            return false;
        }
        this.newQuery = stringOutputStream.toString().replace("\"\"", "\"").trim();
        this.newQuery = this.newQuery.substring(1, this.newQuery.length() - 1);
        String dropQuery = "DROP CUBE " + cubeTableName.toString();
        if (!console.runQuery(queryRunner, dropQuery, outputFormat, schemaChanged, usePager, showProgress, terminal, out, errorChannel)) {
            return false;
        }
        return true;
    }

    public boolean checkCubeName(QueryRunner queryRunner, ReloadCube node, QualifiedName name)
    {
        if (name.getParts().size() > 4) {
            System.err.println("Too many dots in table name");
        }
        List<String> parts = Lists.reverse(name.getParts());
        this.objectName = parts.get(0);
        if (parts.size() > 1) {
            this.schemaName = parts.get(1);
        }
        else {
            if (queryRunner.getSession().getSchema() != null) {
                this.schemaName = queryRunner.getSession().getSchema();
            }
            else {
                System.err.println("Schema must be specified when session schema is not set");
                return false;
            }
        }
        // If there are 4 dots, combine the first two components into one and use it as catalog
        if (parts.size() > 3) {
            if (parts.size() > 3) {
                this.catalogName = parts.get(3) + "." + parts.get(2);
            }
            else {
                if (queryRunner.getSession().getCatalog() != null) {
                    this.catalogName = queryRunner.getSession().getCatalog();
                }
                else {
                    System.err.println("Catalog must be specified when session catalog is not set");
                    return false;
                }
            }
        }
        else {
            if (parts.size() > 2) {
                this.catalogName = parts.get(2);
            }
            else {
                if (queryRunner.getSession().getCatalog() != null) {
                    this.catalogName = queryRunner.getSession().getCatalog();
                }
                else {
                    System.err.println("Catalog must be specified when session catalog is not set");
                    return false;
                }
            }
        }
        return true;
    }
}
