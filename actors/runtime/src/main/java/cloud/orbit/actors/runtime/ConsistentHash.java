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

package cloud.orbit.actors.runtime;

import cloud.orbit.actors.cluster.ClusterNodeView;
import cloud.orbit.actors.cluster.ClusterView;
import cloud.orbit.exception.UncheckedException;

import java.security.MessageDigest;
import java.util.Map;
import java.util.TreeMap;

public class ConsistentHash
{
    private final int shards;

    private volatile TreeMap<String, ClusterNodeView> tree = new TreeMap<>();

    public ConsistentHash(final int shards)
    {
        this.shards = shards;
    }

    public void update ( final ClusterView clusterView )
    {
        final TreeMap<String, ClusterNodeView> newHashes = new TreeMap<>();

        clusterView.listAllRunningServers().stream()
                .forEach(view -> {
                    final String addrHexStr = view.getNodeAddress().asUUID().toString();
                    for (int i = 0; i < shards; ++i ) {
                        final String hash = getHash(addrHexStr + ":" + i);
                        newHashes.put(hash, view);
                    }
                });

        this.tree = newHashes;
    }

    /**
     * Uses consistent hashing to determine the "owner" of a certain key.
     *
     * @param key
     * @return the NodeAddress of the node that's supposed to own the key.
     */
    public ClusterNodeView getConsistentHashOwner(final String key)
    {
        final String keyHash = getHash(key);
        final TreeMap<String, ClusterNodeView> localTree = tree;

        Map.Entry<String, ClusterNodeView> info = localTree.ceilingEntry(keyHash);
        if (info == null)
        {
            info = localTree.firstEntry();
        }
        return info.getValue();
    }

    private String getHash(final String key)
    {
        try
        {
            final MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(key.getBytes("UTF-8"));
            final byte[] digest = md.digest();
            return String.format("%064x", new java.math.BigInteger(1, digest));
        }
        catch (final Exception e)
        {
            throw new UncheckedException(e);
        }
    }

}
