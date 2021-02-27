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
package io.prestosql.security;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.base.security.ImpersonationRule;

import java.util.List;
import java.util.Optional;

public class FileBasedSystemAccessControlRules
{
    private final List<CatalogAccessControlRule> catalogRules;
    private final Optional<List<PrincipalUserMatchRule>> principalUserMatchRules;
    private final List<NodeInformationRule> nodeInfoRules;
    private final List<IndexAccessControlRule> indexRules;
    private final Optional<List<ImpersonationRule>> impersonationRules;

    @JsonCreator
    public FileBasedSystemAccessControlRules(
            @JsonProperty("catalogs") Optional<List<CatalogAccessControlRule>> catalogRules,
            @JsonProperty("principals") Optional<List<PrincipalUserMatchRule>> principalUserMatchRules,
            @JsonProperty("nodeInfo") Optional<List<NodeInformationRule>> nodeInfoRules,
            @JsonProperty("indexAccess") Optional<List<IndexAccessControlRule>> indexRules,
            @JsonProperty("impersonation") Optional<List<ImpersonationRule>> impersonationRules)
    {
        this.catalogRules = catalogRules.map(ImmutableList::copyOf).orElse(ImmutableList.of());
        this.principalUserMatchRules = principalUserMatchRules.map(ImmutableList::copyOf);
        this.nodeInfoRules = nodeInfoRules.map(ImmutableList::copyOf).orElse(ImmutableList.of());
        this.indexRules = indexRules.map(ImmutableList::copyOf).orElse(ImmutableList.of());
        this.impersonationRules = impersonationRules.map(ImmutableList::copyOf);
    }

    public List<CatalogAccessControlRule> getCatalogRules()
    {
        return catalogRules;
    }

    public Optional<List<PrincipalUserMatchRule>> getPrincipalUserMatchRules()
    {
        return principalUserMatchRules;
    }

    public List<NodeInformationRule> getNodeInfoRules()
    {
        return nodeInfoRules;
    }

    public List<IndexAccessControlRule> getIndexRules()
    {
        return indexRules;
    }

    public Optional<List<ImpersonationRule>> getImpersonationRules()
    {
        return impersonationRules;
    }
}
