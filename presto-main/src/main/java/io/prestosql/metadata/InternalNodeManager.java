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
package io.prestosql.metadata;

import io.prestosql.spi.connector.CatalogName;

import java.util.Set;
import java.util.function.Consumer;

public interface InternalNodeManager
{
    Set<InternalNode> getNodes(NodeState state);

    Set<InternalNode> getActiveConnectorNodes(CatalogName catalogName);

    Set<InternalNode> getAllConnectorNodes(CatalogName catalogName);

    InternalNode getCurrentNode();

    Set<InternalNode> getCoordinators();

    AllNodes getAllNodes();

    void refreshNodes();

    void addNodeChangeListener(Consumer<AllNodes> listener);

    void removeNodeChangeListener(Consumer<AllNodes> listener);

    void refreshWorkerStates();
}
