package cloud.orbit.actors.extensions;

import cloud.orbit.actors.cluster.ClusterNodeView;
import cloud.orbit.actors.cluster.NodeAddress;

import java.util.List;

public interface NodeSelectorExtension extends ActorExtension
{
    /**
     * @param interfaceClassName
     * @param localAddress
     * @param potentialNodes Note: contains an entry for the local server, if the local machine is in SERVER/HOST mode.
     * @return
     */
    NodeAddress select(String interfaceClassName, NodeAddress localAddress, List<ClusterNodeView> potentialNodes);

}
