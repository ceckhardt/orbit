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

import cloud.orbit.actors.NodeState;
import cloud.orbit.actors.NodeType;
import cloud.orbit.actors.cluster.ClusterNodeView;
import cloud.orbit.actors.cluster.ClusterPeer;
import cloud.orbit.actors.cluster.ClusterView;
import cloud.orbit.actors.cluster.MessageListener;
import cloud.orbit.actors.cluster.NodeAddress;
import cloud.orbit.actors.cluster.ViewListener;
import cloud.orbit.concurrent.Task;
import cloud.orbit.tuples.Pair;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Fake group networking peer used during unit tests.
 * <p>
 * Using this peer is considerably faster than using jgroups during the tests.
 * </p>
 * It's recommended to use the fake network for application unit tests.
 */
// TODO: Rename this class to FakeClusterChannel
public class FakeClusterPeer implements ClusterPeer
{
    private ViewListener viewListener;
    private MessageListener messageListener;
    private FakeGroup group;
    private NodeAddress address;
    private AtomicLong messagesSent = new AtomicLong();
    private AtomicLong messagesSentOk = new AtomicLong();
    private AtomicLong messagesReceived = new AtomicLong();
    private AtomicLong messagesReceivedOk = new AtomicLong();
    private CompletableFuture<?> startFuture = new CompletableFuture<>();
    private String placementGroup;
    private Set<String> hostableActorInterfaces;

    public FakeClusterPeer()
    {
        this.hostableActorInterfaces = new HashSet<String>() {
            @Override
            public boolean contains(final Object o)
            {
                return true; // just pretend that we accept all actor interfaces so that the tests pass...
            }
        };
    }

    public FakeClusterPeer(Set<String> hostableActorInterfaces)
    {
        this.hostableActorInterfaces = hostableActorInterfaces;
    }

    public Task<Void> join(final String clusterName, final String nodeName, final NodeType nodeType, final String placementGroup)
    {
        this.placementGroup = placementGroup;

        group = FakeGroup.get(clusterName);
        return Task.runAsync(() -> {
            address = group.join(this, nodeType, placementGroup, hostableActorInterfaces);
            startFuture.complete(null);
        }, group.pool());
    }

    @Override
    public Task<?> notifyStateChange(final NodeState newNodeState)
    {
        return Task.done();
    }

    @Override
    public void leave()
    {
        group.leave(this);
    }

    public void onViewChanged(final Map<NodeAddress, ClusterNodeView> newView)
    {
        final SortedMap<NodeAddress, ClusterNodeView> nodeViews = new TreeMap<>(newView);
        viewListener.onViewChange(new ClusterView(nodeViews));
    }

    public void onMessageReceived(final NodeAddress from, final byte[] buff)
    {
        messagesReceived.incrementAndGet();
        messageListener.receive(from, buff);
        messagesReceivedOk.incrementAndGet();
    }

    @Override
    public NodeAddress localAddress()
    {
        return address;
    }

    @Override
    public void registerViewListener(final ViewListener viewListener)
    {
        this.viewListener = viewListener;
    }

    @Override
    public void registerMessageReceiver(final MessageListener messageListener)
    {
        this.messageListener = messageListener;
    }

    @Override
    public void sendMessage(final NodeAddress to, final byte[] message)
    {
        startFuture.join();
        messagesSent.incrementAndGet();
        group.sendMessage(address, to, message).thenRun(() -> messagesSentOk.incrementAndGet());
    }

    @Override
    public <K, V> ConcurrentMap<K, V> getCache(final String name)
    {
        return group.getCache(name);
    }

    void setAddress(final NodeAddress address)
    {
        this.address = address;
    }
}
