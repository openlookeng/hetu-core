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
package io.hetu.core.statestore.hazelcast;

import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import io.airlift.log.Logger;

import java.util.function.Consumer;

/**
 * Membership listener for Hazelcast cluster
 *
 * @since 2020-03-06
 */
public class HazelcastClusterMembershipListener
        implements MembershipListener
{
    private static final Logger log = Logger.get(HazelcastClusterMembershipListener.class);
    private Consumer<String> memberRemovedHandler;

    /**
     * Create HazelcastClusterMembershipListener
     *
     * @param memberRemovedHandler handler to be used when member exit hazelcast state store
     */
    public HazelcastClusterMembershipListener(Consumer<String> memberRemovedHandler)
    {
        this.memberRemovedHandler = memberRemovedHandler;
    }

    @Override
    public void memberAdded(MembershipEvent membershipEvent)
    {
        log.debug("hazelcast member added with host=%s, port=%s",
                membershipEvent.getMember().getAddress().getHost(),
                membershipEvent.getMember().getAddress().getPort());
    }

    @Override
    public void memberRemoved(MembershipEvent membershipEvent)
    {
        log.debug("hazelcast member removed with host=%s, port=%s",
                membershipEvent.getMember().getAddress().getHost(),
                membershipEvent.getMember().getAddress().getPort());

        memberRemovedHandler.accept(
                membershipEvent.getMember().getAddress().getHost()
                + ":"
                + membershipEvent.getMember().getAddress().getPort());
    }
}
