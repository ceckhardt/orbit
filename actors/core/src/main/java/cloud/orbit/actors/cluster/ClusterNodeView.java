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

import cloud.orbit.actors.NodeType;
import cloud.orbit.actors.NodeState;

import java.util.Collections;
import java.util.Set;

/**
 * A <tt>ClusterNodeView</tt> represents the view of some Orbit cluster node (A) as perceived by the local node.
 */
public class ClusterNodeView
{
    private final NodeAddress nodeAddress;
    private final String nodeName;

    private final NodeType nodeType;
    private final NodeState nodeState;

    private final Set<String> hostableActorInterfaces;

    public ClusterNodeView(
            final NodeAddress nodeAddress,
            final String nodeName,
            final NodeType nodeType,
            final NodeState nodeState,
            final Set<String> hostableActorInterfaces
    )
    {
        this.nodeAddress = nodeAddress;
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.nodeState = nodeState;
        this.hostableActorInterfaces = Collections.unmodifiableSet(hostableActorInterfaces);
    }

    public NodeAddress getNodeAddress()
    {
        return nodeAddress;
    }

    public String getNodeName()
    {
        return nodeName;
    }

    public NodeType getNodeType()
    {
        return nodeType;
    }

    public NodeState getNodeState()
    {
        return nodeState;
    }

    public Set<String> getHostableActorInterfaces()
    {
        return hostableActorInterfaces;
    }

    @Override
    public String toString()
    {
        return nodeState + " " + nodeType + " @ " + nodeName;
    }
}
