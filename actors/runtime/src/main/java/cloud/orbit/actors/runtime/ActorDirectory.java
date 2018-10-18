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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import cloud.orbit.actors.cluster.ClusterView;
import cloud.orbit.actors.cluster.NodeAddress;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class ActorDirectory
{
    private final Cache<RemoteReference<?>, NodeAddress> localAddressCache;
    private final Supplier<ConcurrentMap<RemoteKey, NodeAddress>> distributedDirectorySupplier;

    // don't use RemoteReferences, better to restrict keys to a small set of classes.
    private volatile ConcurrentMap<RemoteKey, NodeAddress> distributedDirectory;

    public ActorDirectory(
            final int localAddressCacheMaximumSize,
            final long localAddressCacheTTL,
            final Supplier<ConcurrentMap<RemoteKey, NodeAddress>> distributedDirectorySupplier
    )
    {
        final Caffeine<Object, Object> builder = Caffeine.newBuilder();
        if (localAddressCacheMaximumSize > 0) {
            builder.maximumSize(localAddressCacheMaximumSize);
        }
        builder.expireAfterAccess(localAddressCacheTTL, TimeUnit.MILLISECONDS);
        this.localAddressCache = builder.build();

        this.distributedDirectorySupplier = distributedDirectorySupplier;
    }

    private ConcurrentMap<RemoteKey, NodeAddress> getDistributedDirectory()
    {
        if (distributedDirectory == null)
        {
            synchronized (this)
            {
                if (distributedDirectory == null)
                {
                    distributedDirectory = distributedDirectorySupplier.get();
                }
            }
        }
        return distributedDirectory;
    }

    public void invalidateLocalCache(final RemoteReference<?> remoteReference)
    {
        this.localAddressCache.invalidate(remoteReference);
    }

    public void overwriteLocalCache(final RemoteReference remoteReference, final NodeAddress newAddress)
    {
        this.localAddressCache.put(remoteReference, newAddress);
    }

    private RemoteKey createRemoteKey(final RemoteReference actorReference)
    {
        return new RemoteKey(actorReference._interfaceClass().getName(),
                String.valueOf(actorReference.id));
    }

    public NodeAddress lookup(final ClusterView clusterView, final RemoteReference<?> actorReference)
    {
        final NodeAddress cachedAddress = localAddressCache.getIfPresent(actorReference);
        if ( cachedAddress != null && clusterView.isNodeRunning(cachedAddress) )
        {
            return cachedAddress;
        }

        final NodeAddress retrieved = getDistributedDirectory().get(createRemoteKey(actorReference));
        if ( retrieved != null && clusterView.isNodeRunning(retrieved) ) {
            localAddressCache.put(actorReference, retrieved);
            return retrieved;
        }

        return null;
    }

    public NodeAddress lookupInCache (final ClusterView clusterView, final RemoteReference<?> actorReference)
    {
        final NodeAddress cachedAddress = localAddressCache.getIfPresent(actorReference);
        if ( cachedAddress != null && clusterView.isNodeRunning(cachedAddress) )
        {
            return cachedAddress;
        }

        return null;
    }

    public NodeAddress lookupAndCleanupDistributedIfDead (final ClusterView clusterView, final RemoteReference<?> actorReference)
    {
        final NodeAddress cachedAddress = localAddressCache.getIfPresent(actorReference);
        if ( cachedAddress != null && clusterView.isNodeRunning(cachedAddress) )
        {
            return cachedAddress;
        }

        final RemoteKey remoteKey = createRemoteKey(actorReference);
        final NodeAddress retrieved = getDistributedDirectory().get(remoteKey);
        if ( retrieved != null )
        {
            if ( clusterView.isNodeRunning(retrieved) )
            {
                this.localAddressCache.put(actorReference, retrieved);
                return retrieved;
            }
            else
            {
                // Target node is now dead, remove this activation from distributed cache
                this.getDistributedDirectory().remove(remoteKey, retrieved);
            }
        }

        return null;
    }


    public void actorDeactivated(final RemoteReference<?> actorReference, final NodeAddress localAddress)
    {
        this.localAddressCache.invalidate(actorReference);
        this.getDistributedDirectory().remove(createRemoteKey(actorReference), localAddress);
    }

    public NodeAddress tryPlaceActor(final RemoteReference<?> actorReference, final NodeAddress targetAddress)
    {
        final RemoteKey remoteKey = createRemoteKey(actorReference);

        // Push our selection to the distributed cache (if possible)
        final NodeAddress otherNode = getDistributedDirectory().putIfAbsent(remoteKey, targetAddress);

        if ( otherNode != null )
        {
            // Someone else beat us to placement, use that node
            localAddressCache.put(actorReference, otherNode);
            return otherNode;
        }
        else
        {
            localAddressCache.put(actorReference, targetAddress);
            return targetAddress;
        }
    }
}
