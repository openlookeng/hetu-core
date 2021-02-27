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

import io.prestosql.execution.buffer.OutputBuffer;
import io.prestosql.operator.TaskOutputOperator.TaskOutputOperatorFactory;
import io.prestosql.spi.plan.PlanNodeId;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@Test
public class TestTaskOutputOperatorFactory
{
    @Test
    public void testDuplicate()
    {
        OutputBuffer outputBuffer = mock(OutputBuffer.class);
        TaskOutputOperatorFactory factory1 = new TaskOutputOperatorFactory(
                1,
                new PlanNodeId("planNodeId"),
                outputBuffer,
                a -> a);
        OperatorFactory factory2 = factory1.duplicate();
        OperatorFactory factory3 = factory1.duplicate();
        OperatorFactory factory4 = factory2.duplicate();
        factory1.noMoreOperators();
        factory3.noMoreOperators();
        factory4.noMoreOperators();
        verify(outputBuffer, never()).setNoMoreInputChannels();
        factory2.noMoreOperators();
        verify(outputBuffer).setNoMoreInputChannels();
    }
}
