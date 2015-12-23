---
layout : page
title : "Orbit : Actor Concept - Timers"
breadCrumb : "[Orbit](index.html) / [Public Documentation](orbit-public-documentation.html) / [Actors](orbit-actors.html) / [Actor Concepts](orbit-actor-concepts.html)"
next : "orbit-actor-concept-reminders.html"
previous: "orbit-actor-concept-streams.html"
---
{% include JB/setup %}



-  [Overview](#ActorConcept-Timers-Overview)
-  [Using Timers](#ActorConcept-Timers-UsingTimers)



Overview {#ActorConcept-Timers-Overview}
----------


Timers in Orbit allow a programmer to run a task at set intervals. They are intended to be used for events which fire frequently (milliseconds, seconds).


Timers do not keep an actor activated and will disappear on deactivation.


Timers are usually registered during actor activation, although this is not a requirement.




Using Timers {#ActorConcept-Timers-UsingTimers}
----------

**Timer** 
{% highlight java %}
Registration timer;
 
@Override
public Task activateAsync()
{
    timer = registerTimer(() -> printMessage(), 5, 5, TimeUnit.SECONDS);
    return super.activateAsync();
}
 
public Task printMessage()
{
    System.out.println("Timer Fired");
    timer.dispose();
    timer = null; 
    return Task.done();
}
{% endhighlight %}

-  Registering a timer simply requires calling registerTimer
-  You can store the timer in a Registration object if required
-  Calling dispose on a timer will stop the timer from ticking.