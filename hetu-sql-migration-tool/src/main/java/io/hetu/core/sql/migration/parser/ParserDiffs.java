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
package io.hetu.core.sql.migration.parser;

import org.codehaus.jettison.json.JSONObject;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ParserDiffs
{
    private DiffType diffType;
    private Optional<String> source;
    private Optional<String> sourcePosition;
    private Optional<String> target;
    private Optional<String> targetPosition;
    private Optional<String> message;

    public ParserDiffs(DiffType diffType, Optional<String> source, Optional<String> target, Optional<String> message)
    {
        this.diffType = diffType;
        this.source = source;
        this.sourcePosition = Optional.empty();
        this.target = target;
        this.targetPosition = Optional.empty();
        this.message = message;
    }

    public ParserDiffs(DiffType diffType, Optional<String> source, Optional<String> sourcePosition, Optional<String> target, Optional<String> targetPosition, Optional<String> message)
    {
        this.diffType = diffType;
        this.source = source;
        this.sourcePosition = sourcePosition;
        this.target = target;
        this.targetPosition = targetPosition;
        this.message = message;
    }

    public DiffType getDiffType()
    {
        return diffType;
    }

    public void setDiffType(DiffType diffType)
    {
        this.diffType = diffType;
    }

    public Optional<String> getSource()
    {
        return source;
    }

    public void setSource(String source)
    {
        this.source = Optional.ofNullable(source);
    }

    public Optional<String> getSourcePosition()
    {
        return sourcePosition;
    }

    public void setSourcePosition(String sourcePosition)
    {
        this.sourcePosition = Optional.ofNullable(sourcePosition);
    }

    public Optional<String> getTarget()
    {
        return target;
    }

    public void setTarget(String target)
    {
        this.target = Optional.ofNullable(target);
    }

    public Optional<String> getTargetPosition()
    {
        return targetPosition;
    }

    public void setTargetPosition(String targetPosition)
    {
        this.targetPosition = Optional.ofNullable(targetPosition);
    }

    public Optional<String> getMessage()
    {
        return message;
    }

    public void setMessage(String message)
    {
        this.message = Optional.ofNullable(message);
    }

    public JSONObject toJsonObject()
    {
        Map<String, String> map = new HashMap<>();
        map.put("diffType", String.valueOf(diffType.getValue()));
        map.put("source", source.orElseGet(() -> ""));
        map.put("sourcePosition", sourcePosition.orElseGet(() -> ""));
        map.put("target", target.orElseGet(() -> ""));
        map.put("targetPosition", targetPosition.orElseGet(() -> ""));
        map.put("message", message.orElseGet(() -> ""));

        return new JSONObject(map);
    }
}
