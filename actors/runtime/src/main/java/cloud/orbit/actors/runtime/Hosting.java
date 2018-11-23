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

package cloud.orbit.actors.runtime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cloud.orbit.actors.NodeType;
import cloud.orbit.actors.Stage;
import cloud.orbit.actors.annotation.OnlyIfActivated;
import cloud.orbit.actors.annotation.PreferLocalPlacement;
import cloud.orbit.actors.annotation.StatelessWorker;
import cloud.orbit.actors.cluster.ClusterNodeView;
import cloud.orbit.actors.cluster.ClusterPeer;
import cloud.orbit.actors.cluster.ClusterView;
import cloud.orbit.actors.cluster.NodeAddress;
import cloud.orbit.actors.exceptions.ObserverNotFound;
import cloud.orbit.actors.extensions.NodeSelectorExtension;
import cloud.orbit.actors.extensions.PipelineExtension;
import cloud.orbit.actors.net.HandlerContext;
import cloud.orbit.concurrent.Task;
import cloud.orbit.exception.UncheckedException;
import cloud.orbit.lifecycle.Startable;
import cloud.orbit.util.AnnotationCache;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;

import static com.ea.async.Async.await;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toList;

public class Hosting implements NodeCapabilities, Startable, PipelineExtension
{
    private Logger logger = LoggerFactory.getLogger(Hosting.class);

    private final ActorDirectory actorDirectory;

    private NodeType nodeType;
    private ClusterPeer clusterPeer;

    private Stage stage;

    private final ConsistentHash consistentHash = new ConsistentHash(10);

    private final AnnotationCache<OnlyIfActivated> onlyIfActivateCache = new AnnotationCache<>(OnlyIfActivated.class);

    private CompletableFuture<Void> hostingActive = new Task<>();

    private NodeSelectorExtension nodeSelector;

    private volatile ClusterView clusterView;

    private volatile Set<String> targetPlacementGroups;

    public Hosting(final int localAddressCacheMaximumSize, final long localAddressCacheTTL)
    {
        this.actorDirectory = new ActorDirectory(localAddressCacheMaximumSize, localAddressCacheTTL, this::fetchDistributedDirectory);
    }

    public void setStage(final Stage stage)
    {
        this.stage = stage;
        logger = stage.getLogger(this);
    }

    public void setNodeSelector(NodeSelectorExtension nodeSelector)
    {
        this.nodeSelector = nodeSelector;
    }

    public Set<String> getTargetPlacementGroups()
    {
        return targetPlacementGroups;
    }

    public void setTargetPlacementGroups(Set<String> targetPlacementGroups)
    {
        this.targetPlacementGroups = Collections.unmodifiableSet(targetPlacementGroups);
    }

    public void setNodeType(final NodeType nodeType)
    {
        this.nodeType = nodeType;
    }

    public List<NodeAddress> getAllNodes() {
        final ClusterView localClusterView = this.clusterView;
        if ( localClusterView == null ) {
            return emptyList();
        }

        final List<NodeAddress> nodeAddresses = localClusterView.listAllRunningNodes().stream()
                .map(ClusterNodeView::getNodeAddress)
                .collect(toList());

        return unmodifiableList(nodeAddresses);
    }

    public List<NodeAddress> getServerNodes()
    {
        final ClusterView localClusterView = this.clusterView;
        if ( localClusterView == null ) {
            return emptyList();
        }

        final List<NodeAddress> nodeAddresses = localClusterView.listAllRunningServers().stream()
                .map(ClusterNodeView::getNodeAddress)
                .collect(toList());

        return unmodifiableList(nodeAddresses);
    }

    public List<NodeAddress> getRunningServerNodes() {
        return getServerNodes();
    }

    public Task<?> notifyStateChange()
    {
        return clusterPeer.notifyStateChange(stage.getState());
    }

    public NodeAddress getNodeAddress()
    {
        return clusterPeer.localAddress();
    }

    @Override
    public Task<Void> moved(RemoteReference remoteReference, NodeAddress oldAddress, NodeAddress newAddress)
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("Move {} to from {} to {}.", remoteReference, oldAddress, newAddress);
        }

        actorDirectory.overwriteLocalCache(remoteReference, newAddress);
        return Task.done();
    }

    @Override
    public Task<Void> remove(final RemoteReference<?> remoteReference)
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("Remove {} from this node.", remoteReference);
        }

        actorDirectory.invalidateLocalCache(remoteReference);
        return Task.done();
    }

    public void setClusterPeer(final ClusterPeer clusterPeer)
    {
        this.clusterPeer = clusterPeer;
    }

    @Override
    public Task<Void> start()
    {
        clusterPeer.registerViewListener(this::onClusterViewChanged);
        return Task.done();
    }

    private NodeCapabilities getNodeCapabilities (final NodeAddress nodeAddress) {
        return stage.getRemoteObserverReference(nodeAddress, NodeCapabilities.class, "");
    }

    private synchronized void onClusterViewChanged(final ClusterView newClusterView) {
        final ClusterView oldClusterView = this.clusterView;

        if (logger.isDebugEnabled())
        {
            logger.debug("Cluster view changed " + newClusterView);
        }

        // Logging for the sake of debugging
        for ( final ClusterNodeView view : newClusterView.listAllRunningNodes() ) {
            final ClusterNodeView oldView = oldClusterView == null ? null : oldClusterView.getClusterNodeViews().get(view.getNodeAddress());

            if ( oldView == null ) {
                logger.debug("New node online: {}", view);
                continue;
            }

            if ( view.getNodeState() != oldView.getNodeState() ) {
                logger.debug("Node {} {} transitioned state {} -> {}", view.getNodeAddress(), view.getNodeName(), oldView.getNodeState(), view.getNodeState());
            }

            if ( view.getNodeType() != oldView.getNodeType() ) {
                logger.debug("Node {} {} transitioned type {} -> {}", view.getNodeAddress(), view.getNodeName(), oldView.getNodeType(), view.getNodeType());
            }
        }


        // Update consistentHashNodeTree
        this.consistentHash.update(newClusterView);

        this.clusterView = newClusterView;
    }

    public Task<NodeAddress> locateActor(final RemoteReference reference, final boolean forceActivation)
    {
        final NodeAddress address = RemoteReference.getAddress(reference);
        if (address != null)
        {
            // don't need to call the node call the node to check.
            // checks should be explicit.
            return clusterView.isNodeRunning(address) ? Task.fromValue(address) : Task.fromValue(null);
        }

        return (forceActivation) ? locateAndActivateActor(reference) : locateActiveActor(reference);
    }


    private Task<NodeAddress> locateActiveActor(final RemoteReference<?> actorReference)
    {
        return Task.fromValue(this.actorDirectory.lookup(this.clusterView, actorReference));
    }

    public void actorDeactivated(RemoteReference remoteReference)
    {
        this.actorDirectory.actorDeactivated(remoteReference, getNodeAddress());

        if ( stage.getBroadcastActorDeactivations() ) {
            clusterView.listAllRunningNodes().stream()
                    .filter(view -> ! Objects.equals(view.getNodeAddress(), getNodeAddress()))
                    .forEach(view -> getNodeCapabilities(view.getNodeAddress()).remove(remoteReference));
        }
    }

    private Task<NodeAddress> locateAndActivateActor(final RemoteReference<?> actorReference)
    {
        final Class<?> interfaceClass = ((RemoteReference<?>) actorReference)._interfaceClass();

        // First we handle Stateless Worker as it's a special case
        if (interfaceClass.isAnnotationPresent(StatelessWorker.class))
        {
            // Do we want to place locally?
            if (shouldPlaceLocally(interfaceClass))
            {
                return Task.fromValue(clusterPeer.localAddress());
            }
            else
            {
                return selectNode(interfaceClass.getName());
            }
        }

        // Look for an existing activation of this actor somewhere in the cluster.
        final NodeAddress existingActivationAddress = this.actorDirectory.lookupAndCleanupDistributedIfDead(this.clusterView, actorReference);
        if ( existingActivationAddress != null ) {
            return Task.fromValue(existingActivationAddress);
        }

        // Choose a node to activate the actor on.
        final Task<NodeAddress> selectNodeTask = shouldPlaceLocally(interfaceClass)
                ? Task.fromValue(getNodeAddress())
                : selectNode(interfaceClass.getName());

        // Try to place the actor on that node. Note that we can race with other nodes to choose where to activate this
        // actor. To resolve the race cleanly, the ActorDirectory is required to atomically choose a winner and a loser
        // for the race; the winner places their entry into the ActorDirectory; the loser uses (and returns) that entry.
        return selectNodeTask.thenApply(activationTargetAddress -> this.actorDirectory.tryPlaceActor(actorReference, activationTargetAddress));
    }

    private ConcurrentMap<RemoteKey, NodeAddress> fetchDistributedDirectory()
    {
        return clusterPeer.getCache("distributedDirectory");
    }

    private Task<NodeAddress> selectNode(final String interfaceClassName)
    {
        // Extract volatile fields into local fields to avoid it changing out from under us mid-method.
        final ClusterView localClusterView = this.clusterView;
        final Set<String> targetPlacementGroups = this.targetPlacementGroups;

        // Note: this list can contain the local server as an option.
        final List<ClusterNodeView> eligibleServers = localClusterView.listAllRunningServers().stream()
                .filter(view -> targetPlacementGroups.contains(view.getPlacementGroup()))
                .filter(view -> view.getHostableActorInterfaces().contains(interfaceClassName))
                .collect(toList());

        if ( eligibleServers.isEmpty() ) {
            final String timeoutMessage = "No server capable of handling: " + interfaceClassName;
            logger.error(timeoutMessage);
            return Task.fromException(new UncheckedException(timeoutMessage));
        }

        return Task.fromValue(this.nodeSelector.select(interfaceClassName, getNodeAddress(), eligibleServers));
    }

    private boolean shouldPlaceLocally(final Class<?> interfaceClass)
    {
        final String interfaceClassName = interfaceClass.getName();

        if (interfaceClass.isAnnotationPresent(PreferLocalPlacement.class) &&
                targetPlacementGroups.contains(stage.getPlacementGroup()) &&
                nodeType == NodeType.SERVER && stage.canActivateActor(interfaceClassName))
        {
            final int percentile = interfaceClass.getAnnotation(PreferLocalPlacement.class).percentile();
            if (ThreadLocalRandom.current().nextInt(100) < percentile)
            {
                return true;
            }
        }

        return false;
    }

    /**
     * Uses consistent hashing to determine the "owner" of a certain key.
     *
     * @param key
     * @return the NodeAddress of the node that's supposed to own the key.
     */
    public NodeAddress getConsistentHashOwner(final String key)
    {
        final ClusterNodeView nodeView = consistentHash.getConsistentHashOwner(key);
        return nodeView == null ? null : nodeView.getNodeAddress();
    }

    /**
     * Uses consistent hashing to determine this node is the "owner" of a certain key.
     *
     * @param key
     * @return true if this node is assigned to "own" the key.
     */
    public boolean isConsistentHashOwner(final String key)
    {
        final NodeAddress owner = getConsistentHashOwner(key);
        return clusterPeer.localAddress().equals(owner);
    }

    @Override
    public Task connect(final HandlerContext ctx, final Object param) throws Exception
    {
        return ctx.connect(param);
    }

    @Override
    public void onActive(final HandlerContext ctx) throws Exception
    {
        stage.registerObserver(NodeCapabilities.class, "", this);
        hostingActive.complete(null);
        ctx.fireActive();
    }

    @Override
    public void onRead(final HandlerContext ctx, final Object msg) throws Exception
    {
        if (msg instanceof Invocation)
        {
            onInvocation(ctx, (Invocation) msg);
        }
        else
        {
            ctx.fireRead(msg);
        }
    }

    private Task onInvocation(final HandlerContext ctx, final Invocation invocation)
    {
        final RemoteReference toReference = invocation.getToReference();
        final NodeAddress localAddress = getNodeAddress();

        // Invocation is deliberately addressed to this node.
        if (Objects.equals(toReference.address, localAddress))
        {
            ctx.fireRead(invocation);
            return Task.done();
        }

        // Invocation is addressed to an actor that this node knows that it hosts.
        final NodeAddress cachedAddress = actorDirectory.lookupInCache(clusterView, toReference);
        if (Objects.equals(cachedAddress, localAddress))
        {
            ctx.fireRead(invocation);
            return Task.done();
        }

        // Invocation is for a StatelessWorker, and this node can just activate that StatelessWorker.
        if (toReference._interfaceClass().isAnnotationPresent(StatelessWorker.class)
                && stage.canActivateActor(toReference._interfaceClass().getName()))
        {
            // accepting stateless worker call
            ctx.fireRead(invocation);
            return Task.done();
        }

        // Invocation is @OnlyIfActivated but got sent despite actor deactivation (this is possible even in a healthy
        // cluster via races). Note that since Invocation::method is null on the receiver's side, we need to decode
        // it from the methodId.
        final ObjectInvoker invoker = DefaultDescriptorFactory.get().getInvoker(toReference._interfaceClass());
        final Method method = invoker.getMethod(invocation.getMethodId());
        if (method != null && onlyIfActivateCache.isAnnotated(method))
        {
            logger.debug("Received and discarded @OnlyIfActivated message for non-hosted actor {}", invocation.getToReference());
            invocation.getCompletion().complete(null);
            return Task.done();
        }

        if (logger.isDebugEnabled())
        {
            logger.debug("Choosing a new node for the invocation");
        }

        // over here the actor address is not the localAddress.
        // since we received this message someone thinks that this node is the right one.
        // so we remove that entry from the local cache and query the global cache again
        actorDirectory.invalidateLocalCache(toReference);
        return locateActor(invocation.getToReference(), true)
                .whenComplete((returnedAddress, e) -> {
                    if (e != null)
                    {
                        if (invocation.getCompletion() != null)
                        {
                            if (logger.isDebugEnabled())
                            {
                                logger.debug("Can't find a location for: " + toReference, e);
                            }
                            invocation.getCompletion().completeExceptionally(e);
                        }
                        else
                        {
                            logger.error("Can't find a location for: " + toReference, e);
                        }
                    }
                    else if (Objects.equals(returnedAddress, localAddress))
                    {
                        // accepts the message
                        ctx.fireRead(invocation);
                    }
                    else if (returnedAddress != null)
                    {
                        if (logger.isDebugEnabled())
                        {
                            logger.debug("Choosing a remote node for the invocation");
                        }

                        // Tell the node that sent this Invocation that we aren't hosting that actor.
                        final NodeCapabilities nodeCapabilities = getNodeCapabilities(invocation.getFromNode());
                        nodeCapabilities.moved(toReference, localAddress, returnedAddress);

                        // forwards the message to somewhere else.
                        invocation.setHops(invocation.getHops() + 1);
                        invocation.setToNode(returnedAddress);
                        ctx.write(invocation);
                    }
                    else
                    {
                        // don't know what to do with it...
                        if (logger.isErrorEnabled())
                        {
                            logger.error("Failed to find destination for {}", invocation);
                        }
                    }
                });
    }

    @Override
    public Task write(final HandlerContext ctx, final Object msg) throws Exception
    {
        if (msg instanceof Invocation)
        {
            // await checks isDone()
            await(hostingActive);
            final Invocation invocation = (Invocation) msg;
            if (invocation.getFromNode() == null)
            {
                // used by subsequent filters
                invocation.setFromNode(stage.getLocalAddress());
            }
            if (invocation.getToNode() == null)
            {
                return writeInvocation(ctx, invocation);
            }
        }
        return ctx.write(msg);
    }

    protected Task<?> writeInvocation(final HandlerContext ctx, Invocation invocation)
    {
        final Method method = invocation.getMethod();
        final RemoteReference<?> toReference = invocation.getToReference();

        if (onlyIfActivateCache.isAnnotated(method))
        {
            if (!await(verifyActivated(toReference)))
            {
                return Task.done();
            }
        }
        final Task<?> task;
        if (invocation.getToNode() == null)
        {
            NodeAddress address;
            if ((address = RemoteReference.getAddress(toReference)) != null)
            {
                invocation.withToNode(address);

                if (!clusterView.isNodeRunning(address))
                {
                    return Task.fromException(new ObserverNotFound("Node no longer active"));
                }
                task = ctx.write(invocation);
            }
            else
            {
                // TODO: Ensure that both paths encode exception the same way.
                address = await(locateActor(toReference, true));
                task = ctx.write(invocation.withToNode(address));
            }
        }
        else
        {
            task = ctx.write(invocation);
        }
        return task.whenCompleteAsync((r, e) ->
                {
                    // place holder, just to ensure the completion happens in another thread
                },
                stage.getExecutionPool());
    }

    /**
     * Checks if the object is activated.
     */
    private Task<Boolean> verifyActivated(RemoteReference<?> toReference)
    {
        return locateActor(toReference, false).thenApply(Objects::nonNull);
    }
}
