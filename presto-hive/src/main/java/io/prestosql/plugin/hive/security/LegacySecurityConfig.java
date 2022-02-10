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
package io.prestosql.plugin.hive.security;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

public class LegacySecurityConfig
{
    private boolean allowAddColumn;
    private boolean allowDropColumn;
    private boolean allowDropTable;
    private boolean allowRenameTable;
    private boolean allowCommentTable;
    private boolean allowRenameColumn;
    private boolean allowDropPartition;

    public boolean getAllowAddColumn()
    {
        return this.allowAddColumn;
    }

    @Config("hive.allow-add-column")
    @ConfigDescription("Allow Hive connector to add column")
    public LegacySecurityConfig setAllowAddColumn(boolean allowAddColumn)
    {
        this.allowAddColumn = allowAddColumn;
        return this;
    }

    public boolean getAllowDropColumn()
    {
        return this.allowDropColumn;
    }

    @Config("hive.allow-drop-column")
    @ConfigDescription("Allow Hive connector to drop column")
    public LegacySecurityConfig setAllowDropColumn(boolean allowDropColumn)
    {
        this.allowDropColumn = allowDropColumn;
        return this;
    }

    public boolean getAllowDropTable()
    {
        return this.allowDropTable;
    }

    @Config("hive.allow-drop-table")
    @ConfigDescription("Allow Hive connector to drop table")
    public LegacySecurityConfig setAllowDropTable(boolean allowDropTable)
    {
        this.allowDropTable = allowDropTable;
        return this;
    }

    public boolean getAllowRenameTable()
    {
        return this.allowRenameTable;
    }

    @Config("hive.allow-rename-table")
    @ConfigDescription("Allow Hive connector to rename table")
    public LegacySecurityConfig setAllowRenameTable(boolean allowRenameTable)
    {
        this.allowRenameTable = allowRenameTable;
        return this;
    }

    public boolean getAllowCommentTable()
    {
        return this.allowCommentTable;
    }

    @Config("hive.allow-comment-table")
    @ConfigDescription("Allow Hive connector to set comment for a table")
    public LegacySecurityConfig setAllowCommentTable(boolean allowCommentTable)
    {
        this.allowCommentTable = allowCommentTable;
        return this;
    }

    public boolean getAllowRenameColumn()
    {
        return this.allowRenameColumn;
    }

    @Config("hive.allow-rename-column")
    @ConfigDescription("Allow Hive connector to rename column")
    public LegacySecurityConfig setAllowRenameColumn(boolean allowRenameColumn)
    {
        this.allowRenameColumn = allowRenameColumn;
        return this;
    }

    public boolean getAllowDropPartition()
    {
        return this.allowDropPartition;
    }

    @Config("hive.allow-drop-partition")
    @ConfigDescription("Allow Hive connector to drop partitions")
    public LegacySecurityConfig setAllowDropPartition(boolean allowDropPartition)
    {
        this.allowDropPartition = allowDropPartition;
        return this;
    }
}
