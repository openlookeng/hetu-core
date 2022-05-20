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
package io.prestosql.server;

import io.airlift.log.Logger;
import io.prestosql.failuredetector.WorkerGossipFailureDetector;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

import static io.prestosql.server.GossipStatusResource.GOSSIP_STATUS;
import static java.util.Objects.requireNonNull;

@Path(GOSSIP_STATUS)
public class GossipStatusResource
{
    private final WorkerGossipFailureDetector failureDetector;
    public static final String GOSSIP_STATUS = "/gossip/status";
    private static final Logger log = Logger.get(GossipStatusResource.class);

    @Inject
    public GossipStatusResource(WorkerGossipFailureDetector failureDetector)
    {
        this.failureDetector = failureDetector;
    }

    @GET
    public Response getAliveBitmap()
    {
        byte[] bitset = this.failureDetector.getAliveNodesBitmap().toByteArray();
        return Response.status(Response.Status.OK).entity(bitset).build();
    }

    @POST
    public Response postWorkersToGossip(byte[] whomToGossipInfo)
    {
        requireNonNull(whomToGossipInfo, "whomToGossipInfo is null");
        this.failureDetector.initWhomToGossipList(whomToGossipInfo);
        log.debug("init gossip at worker..");
        return Response.ok().build();
    }
}
