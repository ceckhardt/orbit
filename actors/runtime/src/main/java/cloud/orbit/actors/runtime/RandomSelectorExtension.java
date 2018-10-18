package cloud.orbit.actors.runtime;

import cloud.orbit.actors.cluster.ClusterNodeView;
import cloud.orbit.actors.cluster.NodeAddress;
import cloud.orbit.actors.extensions.NodeSelectorExtension;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class RandomSelectorExtension implements NodeSelectorExtension
{

    @Override
    public NodeAddress select(final String interfaceClassName, final NodeAddress localAddress, final List<ClusterNodeView> potentialNodes)
    {
      return potentialNodes.get(ThreadLocalRandom.current().nextInt(potentialNodes.size())).getNodeAddress();
    }

}
