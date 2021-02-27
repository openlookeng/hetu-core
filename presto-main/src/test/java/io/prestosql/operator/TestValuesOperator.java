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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.MarkerPage;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.SessionTestUtils.TEST_SNAPSHOT_SESSION;
import static io.prestosql.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestValuesOperator
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    private OperatorContext mockOperatorContext(boolean snapshot)
    {
        return createTaskContext(executor, scheduledExecutor, snapshot ? TEST_SNAPSHOT_SESSION : TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext()
                .addOperatorContext(1, new PlanNodeId("1"), "Values");
    }

    @Test
    public void testNormal()
    {
        List<Page> pages = ImmutableList.of(new Page(1));
        ValuesOperator operator = new ValuesOperator(mockOperatorContext(false), pages, 0);
        Page page = operator.getOutput();
        assertNotNull(page);
        assertFalse(page instanceof MarkerPage);
        page = operator.getOutput();
        assertNull(page);
    }

    @Test
    public void testNormalPeekMarker()
    {
        List<Page> pages = ImmutableList.of(new Page(1));
        ValuesOperator operator = new ValuesOperator(mockOperatorContext(false), pages, 0);
        Page page = operator.pollMarker();
        assertNull(page);
        page = operator.getOutput();
        assertNotNull(page);
        assertFalse(page instanceof MarkerPage);
        page = operator.pollMarker();
        assertNull(page);
        page = operator.getOutput();
        assertNull(page);
    }

    @Test
    public void testSnapshot()
    {
        List<Page> pages = ImmutableList.of(new Page(1), MarkerPage.snapshotPage(1));
        ValuesOperator operator = new ValuesOperator(mockOperatorContext(true), pages, 0);
        Page page = operator.getOutput();
        assertNotNull(page);
        assertFalse(page instanceof MarkerPage);
        page = operator.getOutput();
        assertTrue(page instanceof MarkerPage);
        page = operator.getOutput();
        assertNull(page);
    }

    @Test
    public void testSnapshotPeekMarker()
    {
        List<Page> pages = ImmutableList.of(new Page(1), MarkerPage.snapshotPage(1));
        ValuesOperator operator = new ValuesOperator(mockOperatorContext(true), pages, 0);
        Page page = operator.pollMarker();
        assertNull(page);
        page = operator.getOutput();
        assertNotNull(page);
        assertFalse(page instanceof MarkerPage);
        page = operator.pollMarker();
        assertTrue(page instanceof MarkerPage);
        page = operator.pollMarker();
        assertNull(page);
        page = operator.getOutput();
        assertNull(page);
    }

    @Test
    public void testResume()
    {
        List<Page> pages = ImmutableList.of(new Page(1), MarkerPage.resumePage(1, 1), MarkerPage.snapshotPage(2));
        ValuesOperator operator = new ValuesOperator(mockOperatorContext(true), pages, 1);
        Page page = operator.getOutput();
        assertTrue(page instanceof MarkerPage && ((MarkerPage) page).isResuming());
        page = operator.getOutput();
        assertTrue(page instanceof MarkerPage && !((MarkerPage) page).isResuming());
        page = operator.getOutput();
        assertNull(page);
    }

    @Test
    public void testResumePeekMarker()
    {
        List<Page> pages = ImmutableList.of(new Page(1), MarkerPage.resumePage(1, 1), MarkerPage.snapshotPage(2));
        ValuesOperator operator = new ValuesOperator(mockOperatorContext(true), pages, 1);
        Page page = operator.pollMarker();
        assertTrue(page instanceof MarkerPage && ((MarkerPage) page).isResuming());
        page = operator.pollMarker();
        assertTrue(page instanceof MarkerPage && !((MarkerPage) page).isResuming());
        page = operator.pollMarker();
        assertNull(page);
        page = operator.getOutput();
        assertNull(page);
    }
}
