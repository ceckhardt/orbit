/*
 Copyright (C) 2016 Electronic Arts Inc.  All rights reserved.

 Redistribution and use in source and binary forms, with or without
 modification, are permitted provided that the following conditions
 are met:

 1.  Redistributions of source code must retain the above copyright
     notice, this list of conditions and the following disclaimer.
 2.  Redistributions in binary form must reproduce the above copyright
     notice, this list of conditions and the following disclaimer in the
     documentation and/or other materials provided with the distribution.
 3.  Neither the name of Electronic Arts, Inc. ("EA") nor the names of
     its contributors may be used to endorse or promote products derived
     from this software without specific prior written permission.

 THIS SOFTWARE IS PROVIDED BY ELECTRONIC ARTS AND ITS CONTRIBUTORS "AS IS" AND ANY
 EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 DISCLAIMED. IN NO EVENT SHALL ELECTRONIC ARTS OR ITS CONTRIBUTORS BE LIABLE FOR ANY
 DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package cloud.orbit.actors.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

import cloud.orbit.actors.NodeState;
import cloud.orbit.actors.NodeType;
import cloud.orbit.actors.cluster.ClusterNodeView;
import cloud.orbit.actors.cluster.NodeAddress;
import cloud.orbit.actors.cluster.NodeAddressImpl;
import cloud.orbit.concurrent.ExecutorUtils;
import cloud.orbit.concurrent.Task;
import cloud.orbit.exception.UncheckedException;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

public class FakeGroup
{
    private static final Logger logger = LoggerFactory.getLogger(FakeGroup.class);

    private static final LoadingCache<String, FakeGroup> groups = Caffeine.newBuilder()
            .weakValues()
            .build(new CacheLoader<String, FakeGroup>()
            {
                @Override
                public FakeGroup load(final String key) throws Exception
                {
                    return new FakeGroup(key);
                }
            });

    private final Map<NodeAddress, FakeClusterPeer> currentChannels = new HashMap<>();
    private final Map<NodeAddress, ClusterNodeView> fakeNodeViews = new HashMap<>();

    private final Object topologyMutex = new Object();
    @SuppressWarnings("rawtypes")
    private final LoadingCache<String, ConcurrentMap> maps = Caffeine.newBuilder()
            .build(new CacheLoader<String, ConcurrentMap>()
            {
                @Override
                public ConcurrentMap load(final String key) throws Exception
                {
                    return new ConcurrentHashMap();
                }
            });
    private int count = 0;
    private String clusterName;
    private static Executor pool = ExecutorUtils.newScalingThreadPool(20);

    public FakeGroup(final String clusterName)
    {
        this.clusterName = clusterName;
    }

    protected NodeAddressImpl join(final FakeClusterPeer fakeChannel, final NodeType nodeType, final String placementGroup, final Set<String> hostableActorInterfaces)
    {
        Collection<CompletableFuture<?>> tasks;
        NodeAddressImpl nodeAddress;
        synchronized (topologyMutex)
        {
            final String name = "channel." + (++count) + "." + clusterName;
            nodeAddress = new NodeAddressImpl(new UUID(name.hashCode(), count));
            currentChannels.put(nodeAddress, fakeChannel);
            fakeNodeViews.put(nodeAddress, new ClusterNodeView(nodeAddress, "fake-node-name", nodeType, NodeState.RUNNING, placementGroup, hostableActorInterfaces));
            fakeChannel.setAddress(nodeAddress);

            tasks = currentChannels.values().stream().map(ch -> CompletableFuture.runAsync(() -> ch.onViewChanged(fakeNodeViews), pool)).collect(Collectors.toList());
        }
        Task.allOf(tasks).join();

        return nodeAddress;
    }

    public void leave(final FakeClusterPeer fakeClusterPeer)
    {
        List<CompletableFuture<?>> tasks;
        synchronized (topologyMutex)
        {
            currentChannels.remove(fakeClusterPeer.localAddress());
            final ClusterNodeView view = fakeNodeViews.get(fakeClusterPeer.localAddress());
            final ClusterNodeView updated = new ClusterNodeView(view.getNodeAddress(), view.getNodeName(), view.getNodeType(), NodeState.STOPPED, view.getPlacementGroup(), Collections.emptySet());
            fakeNodeViews.put(view.getNodeAddress(), updated);
            tasks = currentChannels.values().stream().map(ch -> CompletableFuture.runAsync(() -> ch.onViewChanged(fakeNodeViews), pool)).collect(Collectors.toList());
        }
        Task.allOf(tasks).join();
    }


    public Task<Void> sendMessage(final NodeAddress from, final NodeAddress to, final byte[] buff)
    {
        if (to == null)
        {
            throw new NullPointerException("Target address cannot be null");
        }
        return Task.runAsync(() -> {
            try
            {
                final FakeClusterPeer fakeClusterPeer = currentChannels.get(to);
                if (fakeClusterPeer == null)
                {
                    throw new UncheckedException("Unknown address: " + to);
                }
                fakeClusterPeer.onMessageReceived(from, buff);
            }
            catch (Exception ex)
            {
                logger.error("Error sending message", ex);
            }
        }, pool);
    }

    public static FakeGroup get(final String clusterName)
    {
        return groups.get(clusterName);
    }

    @SuppressWarnings("unchecked")
    public <K, V> ConcurrentMap<K, V> getCache(final String name)
    {
        return maps.get(name);
    }

    public Map<String, ConcurrentMap> getCaches()
    {
        return maps.asMap();
    }

    Executor pool()
    {
        return pool;
    }
}
