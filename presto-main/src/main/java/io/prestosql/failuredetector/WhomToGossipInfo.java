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
package io.prestosql.failuredetector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.log.Logger;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class WhomToGossipInfo
{
    private static final Logger log = Logger.get(WhomToGossipInfo.class);
    private List<URI> uri = new ArrayList<>();

    @JsonCreator
    public WhomToGossipInfo()
    {
    }

    @JsonCreator
    public WhomToGossipInfo(String list)
    {
        requireNonNull(list, "list is null");
        String[] uris = list.split(",");
        Arrays.stream(uris).filter(u -> !u.isEmpty()).forEach(u -> {
            try {
                log.debug("adding uri: " + uri);
                add(new URI(u));
            }
            catch (URISyntaxException e) {
                log.error("failed to create object");
            }
        });
    }

    @JsonCreator
    public WhomToGossipInfo(@JsonProperty("uri") URI uri)
    {
        requireNonNull(uri, "uri is null");
        this.uri.add(uri);
    }

    @JsonCreator
    public WhomToGossipInfo(@JsonProperty("uri") List<URI> uri)
    {
        requireNonNull(uri, "uri is null");
        this.uri.addAll(uri);
    }

    public void add(URI uri)
    {
        requireNonNull(uri, "uri is null");
        this.uri.add(uri);
    }

    public List<URI> getUriList()
    {
        return this.uri;
    }

    @JsonProperty
    public String getUri()
    {
        StringBuilder sb = new StringBuilder();
        for (URI u : this.uri) {
            sb.append(u.toString()).append(",");
        }
        String s = sb.toString();
        String concatUri = (s.length() > 0) ? s.substring(0, s.length() - 1) : "";
        log.debug("uri: " + concatUri);
        return concatUri;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("uri", getUri())
                .toString();
    }
}
