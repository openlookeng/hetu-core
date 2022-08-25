/*
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
package io.prestosql.plugin.hive.metastore.glue;

import com.amazonaws.services.glue.model.BatchCreatePartitionRequest;
import com.amazonaws.services.glue.model.BatchGetPartitionRequest;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.DeletePartitionRequest;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.GetDatabaseRequest;
import com.amazonaws.services.glue.model.GetDatabasesRequest;
import com.amazonaws.services.glue.model.GetPartitionRequest;
import com.amazonaws.services.glue.model.GetPartitionsRequest;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.UpdateDatabaseRequest;
import com.amazonaws.services.glue.model.UpdatePartitionRequest;
import com.amazonaws.services.glue.model.UpdateTableRequest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GlueCatalogIdRequestHandlerTest
{
    private GlueCatalogIdRequestHandler glueCatalogIdRequestHandlerUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        glueCatalogIdRequestHandlerUnderTest = new GlueCatalogIdRequestHandler("catalogId");
    }

    @Test
    public void testBeforeExecution()
    {
        glueCatalogIdRequestHandlerUnderTest.beforeExecution(new GetDatabasesRequest());
        glueCatalogIdRequestHandlerUnderTest.beforeExecution(new GetDatabaseRequest());
        glueCatalogIdRequestHandlerUnderTest.beforeExecution(new CreateDatabaseRequest());
        glueCatalogIdRequestHandlerUnderTest.beforeExecution(new UpdateDatabaseRequest());
        glueCatalogIdRequestHandlerUnderTest.beforeExecution(new DeleteDatabaseRequest());
        glueCatalogIdRequestHandlerUnderTest.beforeExecution(new GetTablesRequest());
        glueCatalogIdRequestHandlerUnderTest.beforeExecution(new GetTableRequest());
        glueCatalogIdRequestHandlerUnderTest.beforeExecution(new CreateTableRequest());
        glueCatalogIdRequestHandlerUnderTest.beforeExecution(new UpdateTableRequest());
        glueCatalogIdRequestHandlerUnderTest.beforeExecution(new DeleteTableRequest());
        glueCatalogIdRequestHandlerUnderTest.beforeExecution(new GetPartitionsRequest());
        glueCatalogIdRequestHandlerUnderTest.beforeExecution(new GetPartitionRequest());
        glueCatalogIdRequestHandlerUnderTest.beforeExecution(new UpdatePartitionRequest());
        glueCatalogIdRequestHandlerUnderTest.beforeExecution(new DeletePartitionRequest());
        glueCatalogIdRequestHandlerUnderTest.beforeExecution(new BatchGetPartitionRequest());
        glueCatalogIdRequestHandlerUnderTest.beforeExecution(new BatchCreatePartitionRequest());
    }
}
