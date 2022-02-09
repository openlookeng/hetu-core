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

package io.prestosql.tests;

import static io.prestosql.tempto.query.QueryExecutor.query;
import static java.lang.String.format;

public class TestCarbondataConcurrent
        extends Thread
{
    private String tableName;
    private CreateTableTests.CarbonTaskOperation taskOperation;

    public TestCarbondataConcurrent(String tableName, CreateTableTests.CarbonTaskOperation taskOperation)
    {
        super.setName("TestCarbondataConcurrent");
        this.tableName = tableName;
        this.taskOperation = taskOperation;
    }

    public void run()
    {
        switch (taskOperation) {
            case CARBON_CREATE_TABLE:
            {
                testCreateTable();
                break;
            }
            case CARBON_DROP_TABLE:
            {
                testDropTable();
                break;
            }
        }
    }

    void testCreateTable()
    {
        try {
            query(format("CREATE TABLE %s(a int)", tableName));
            // Displaying the thread that is running
            System.out.println(" create table Thread " + Thread.currentThread().getId() + " is running");
        }
        catch (Exception e) {
            // Throwing an exception
            System.out.println("Exception is caught " + e.getMessage());
        }
    }

    void testDropTable()
    {
        try {
            query(format("DROP TABLE %s", tableName));
            // Displaying the thread that is running
            System.out.println(" Drop table Thread " + Thread.currentThread().getId() + " is running");
        }
        catch (Exception e) {
            // Throwing an exception
            System.out.println("Exception is caught " + e.getMessage());
        }
    }
}
