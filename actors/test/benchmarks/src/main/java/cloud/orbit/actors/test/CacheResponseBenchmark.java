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

package cloud.orbit.actors.test;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import cloud.orbit.actors.Actor;
import cloud.orbit.actors.Stage;
import cloud.orbit.actors.annotation.CacheResponse;
import cloud.orbit.actors.cache.ExecutionCacheFlushManager;
import cloud.orbit.actors.extensions.json.InMemoryJSONStorageExtension;
import cloud.orbit.actors.runtime.AbstractActor;
import cloud.orbit.actors.util.IdUtils;
import cloud.orbit.concurrent.Task;

import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@Fork(1)
@BenchmarkMode(Mode.Throughput)
@Threads(1)
public class CacheResponseBenchmark
{
    private static final int MAX_ACTORS = 5000;

    private static final int MAX_PARAMS = 10;

    private Stage stage;

    private int actorId = 0;

    private int param = 0;

    public interface Hello extends Actor
    {
        @CacheResponse(maxEntries = 20_000_000, ttlDuration = 1, ttlUnit = TimeUnit.DAYS)
        Task<String> getGreetingCached(final String param);

        Task<String> getGreeting(final String param);

        @CacheResponse(maxEntries = 20_000_000, ttlDuration = 1, ttlUnit = TimeUnit.DAYS)
        Task<String> getGreetingWithDelayCached(final String param);

        Task<String> getGreetingWithDelay(final String param);

    }

    public static class HelloActor extends AbstractActor implements Hello
    {

        @Override
        public Task<String> getGreetingCached(final String param)
        {
            return Task.fromValue(IdUtils.urlSafeString(32));
        }

        @Override
        public Task<String> getGreeting(final String param)
        {
            return Task.fromValue(IdUtils.urlSafeString(32));
        }

        @Override
        public Task<String> getGreetingWithDelayCached(final String param)
        {
            return Task.sleep(1, TimeUnit.MILLISECONDS).thenApply(v -> IdUtils.urlSafeString(32));
        }

        @Override
        public Task<String> getGreetingWithDelay(final String param)
        {
            return Task.sleep(1, TimeUnit.MILLISECONDS).thenApply(v -> IdUtils.urlSafeString(32));
        }
    }

    @Setup
    public void setup()
    {
        System.setProperty("java.net.preferIPv4Stack", "true");
        this.stage = new Stage.Builder()
                .extensions(new InMemoryJSONStorageExtension(new ConcurrentHashMap<>()))
                .mode(Stage.StageMode.HOST)
                .clusterName(IdUtils.urlSafeString(32))
                .executionPoolSize(Math.max(1, Runtime.getRuntime().availableProcessors() / 2))
                .build();
        this.stage.start().join();
        this.stage.bind();
        this.actorId = 0;
        this.param = 0;
    }

    @TearDown
    public void tearDown()
    {
        this.stage.stop().join();
    }

    @Benchmark
    @Warmup(iterations = 100)
    public void get(final CacheResponseBenchmark state)
    {
        Actor.getReference(Hello.class, Objects.toString(state.actorId))
                .getGreetingWithDelay(Objects.toString(state.param)).join();
        state.param++;
        if (state.param >= 10)
        {
            state.actorId++;
            state.param = 0;
            if (state.actorId >= MAX_ACTORS)
            {
                state.actorId = 0;
            }
        }
    }

    @Benchmark
    @Warmup(iterations = 100)
    public void getCached(final CacheResponseBenchmark state)
    {
        Actor.getReference(Hello.class, Objects.toString(state.actorId))
                .getGreetingWithDelayCached(Objects.toString(state.param)).join();
        state.param++;
        if (state.param >= 10)
        {
            state.actorId++;
            state.param = 0;
            if (state.actorId >= MAX_ACTORS)
            {
                state.actorId = 0;
            }
        }
    }

    @Benchmark
    public void put(final CacheResponseBenchmark state)
    {
        Actor.getReference(Hello.class, Objects.toString(state.actorId))
                .getGreeting(Objects.toString(state.param)).join();
        state.param++;
        if (state.param >= MAX_PARAMS)
        {
            state.actorId++;
            state.param = 0;
        }
    }

    @Benchmark
    public void putCached(final CacheResponseBenchmark state)
    {
        Actor.getReference(Hello.class, Objects.toString(state.actorId))
                .getGreetingCached(Objects.toString(state.param)).join();
        state.param++;
        if (state.param >= MAX_PARAMS)
        {
            state.actorId++;
            state.param = 0;
        }
    }

}
