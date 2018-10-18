/*
 Copyright (C) 2018 Electronic Arts Inc.  All rights reserved.

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

package cloud.orbit.actors.cluster;

import cloud.orbit.actors.NodeState;
import cloud.orbit.actors.NodeType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import static java.util.stream.Collectors.toList;

/**
 * A <tt>ClusterView</tt> is an immutable view of all Orbit nodes in a single cluster, possibly including a number of
 * dead or temporarily-down nodes, as well as stopping or stopped nodes.
 *
 * A <tt>ClusterView</tt> is provided to Orbit by an implementation of <tt>ClusterPeer</tt>. Orbit bases all of its
 * decisions around e.g. whether an actor needs to be restarted on its own <tt>ClusterView</tt> at the time.
 */
public class ClusterView
{
    // Note: there is no strict requirement that `clusterNodeViews` be sorted from the perspective of Orbit.
    // But it's a lot nicer for debugging output.
    private final SortedMap<NodeAddress, ClusterNodeView> clusterNodeViews;
    private final List<ClusterNodeView> allRunningNodes;
    private final List<ClusterNodeView> allRunningServers;

    public ClusterView(final SortedMap<NodeAddress, ClusterNodeView> clusterNodeViews)
    {
        this.clusterNodeViews = Collections.unmodifiableSortedMap(clusterNodeViews);

        this.allRunningNodes = clusterNodeViews.values().stream()
                .filter(view -> view.getNodeState() == NodeState.RUNNING)
                .collect(toList());

        this.allRunningServers = this.allRunningNodes.stream()
                .filter(view -> view.getNodeType() == NodeType.SERVER)
                .collect(toList());
    }

    public Map<NodeAddress, ClusterNodeView> getClusterNodeViews()
    {
        return clusterNodeViews;
    }

    public List<ClusterNodeView> listAllRunningNodes()
    {
        return allRunningNodes;
    }

    public List<ClusterNodeView> listAllRunningServers()
    {
        return allRunningServers;
    }

    public boolean isNodeRunning ( final NodeAddress address )
    {
        // A node is considered to be running if and only if we know about it and have received a recent heartbeat from
        // it, and we know it to be running.
        final ClusterNodeView clusterNodeView = clusterNodeViews.get(address);
        return clusterNodeView != null && clusterNodeView.getNodeState() == NodeState.RUNNING;
    }

    @Override
    public String toString()
    {
        StringBuilder buf = new StringBuilder("ClusterView\n");
        clusterNodeViews.forEach((addr, view) -> buf.append('\t').append(view).append('\n'));
        return buf.toString();
    }
}
