package cloud.orbit.actors.extensions;

import cloud.orbit.actors.runtime.AbstractActor;

/**
 * Listener for actor activation/deactivation proximal cause (i.e. which invocation forced an actor to be awoken).
 */
public interface ActivationReasonExtension extends ActorExtension
{

    /**
     * Called when an actor has been activated, with a given reason.
     *
     * @param actor the actor object being activated
     */
    void onActivation(AbstractActor<?> actor, String methodName);
}
