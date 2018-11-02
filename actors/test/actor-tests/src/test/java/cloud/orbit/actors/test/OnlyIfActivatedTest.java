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

import cloud.orbit.actors.Actor;
import cloud.orbit.actors.Stage;
import cloud.orbit.actors.test.actors.OnlyIfActivated;
import cloud.orbit.actors.test.actors.OnlyIfActivatedActor;
import cloud.orbit.exception.UncheckedException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class OnlyIfActivatedTest extends ActorBaseTest
{
    @Test
    public void onlyIfActivatedTest()
    {
        final Stage stage = createStage();
        clock.stop();
        OnlyIfActivated only = Actor.getReference(OnlyIfActivated.class, "234");
        only.doSomethingSpecial("A").join();
        only.doSomethingSpecial("A").join();
        only.doSomethingSpecial("A").join();
        only.doSomethingSpecial("A").join();
        only.doSomethingSpecial("A").join();
        Assert.assertEquals(0, OnlyIfActivatedActor.accessCount);
        only.makeActiveNow().join();
        only.doSomethingSpecial("A").join();
        only.doSomethingSpecial("A").join();
        only.doSomethingSpecial("A").join();
        only.doSomethingSpecial("A").join();
        only.doSomethingSpecial("A").join();
        assertEquals(5, OnlyIfActivatedActor.accessCount);

        clock.incrementTime(10, TimeUnit.MINUTES);
        only.doSomethingSpecial("A").join();
        assertEquals(6, OnlyIfActivatedActor.accessCount);
        clock.incrementTime(10, TimeUnit.MINUTES);
        stage.cleanup().join();
        only.doSomethingSpecial("A").join();
        assertEquals(6, OnlyIfActivatedActor.accessCount);
    }

    @Test
    public void onlyIfActivatedClusterTest()
    {
        // test what happens if an invocation arrives at a node for an @OnlyIfActivated method for an inactive actor
        clock.stop();

        final Stage stage1 = createStage();
        stage1.bind();

        final OnlyIfActivated only = Actor.getReference(OnlyIfActivated.class, "345");
        only.makeActiveNow().join();
        only.doSomethingSpecial("A").join();
        assertEquals(1, OnlyIfActivatedActor.accessCount);

        final Stage stage2 = createStage();
        stage2.bind();
        only.doSomethingSpecial("B").join();
        assertEquals(2, OnlyIfActivatedActor.accessCount);

        clock.incrementTime(11, TimeUnit.MINUTES);
        stage1.cleanup().join();
        stage1.bind();
        assertEquals(1, OnlyIfActivatedActor.deactivationCount);
        assertEquals(null, only.doSomethingSpecial("C").join());

        // OnlyIfActivatedActor:345 should now be deactivated on stage1, but still in ActorDirectory cache on stage2.
        // So stage2 can still issue Invocations for that actor to stage1. Stage1 should discard them.
        stage2.bind();
        assertEquals(null, only.doSomethingSpecial("D").join());
        assertEquals(2, OnlyIfActivatedActor.accessCount);
    }

    @Before
    public void initializeStage()
    {
        try
        {
            OnlyIfActivatedActor.accessCount = 0;
            OnlyIfActivatedActor.deactivationCount = 0;
        }
        catch (Exception e)
        {
            throw new UncheckedException(e);
        }
    }
}
