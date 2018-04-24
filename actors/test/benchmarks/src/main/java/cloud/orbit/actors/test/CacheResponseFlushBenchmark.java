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
import org.openjdk.jmh.annotations.Level;
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
import cloud.orbit.actors.runtime.ActorProfiler;
import cloud.orbit.actors.util.IdUtils;
import cloud.orbit.concurrent.Task;
import cloud.orbit.profiler.ProfileDump;
import cloud.orbit.profiler.ProfilerData;

import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@Fork(1)
@BenchmarkMode(Mode.Throughput)
@Threads(1)
public class CacheResponseFlushBenchmark
{
    private static final int NUM_PARAMS = 10;

    private Stage stage;

    private boolean warmCache = false;

    private int actorId = 0;

    public interface Hello extends Actor
    {
        @CacheResponse(maxEntries = 20_000_000, ttlDuration = 1, ttlUnit = TimeUnit.DAYS)
        Task<String> getGreeting(final String param);

        @CacheResponse(maxEntries = 20_000_000, ttlDuration = 1, ttlUnit = TimeUnit.DAYS)
        Task<String> getGreeting1();

        @CacheResponse(maxEntries = 20_000_000, ttlDuration = 1, ttlUnit = TimeUnit.DAYS)
        Task<String> getGreeting2();

        @CacheResponse(maxEntries = 20_000_000, ttlDuration = 1, ttlUnit = TimeUnit.DAYS)
        Task<String> getGreeting3();

        @CacheResponse(maxEntries = 20_000_000, ttlDuration = 1, ttlUnit = TimeUnit.DAYS)
        Task<String> getGreeting4();

        @CacheResponse(maxEntries = 20_000_000, ttlDuration = 1, ttlUnit = TimeUnit.DAYS)
        Task<String> getGreeting5();

        @CacheResponse(maxEntries = 20_000_000, ttlDuration = 1, ttlUnit = TimeUnit.DAYS)
        Task<String> getGreeting6();

        @CacheResponse(maxEntries = 20_000_000, ttlDuration = 1, ttlUnit = TimeUnit.DAYS)
        Task<String> getGreeting7();

        @CacheResponse(maxEntries = 20_000_000, ttlDuration = 1, ttlUnit = TimeUnit.DAYS)
        Task<String> getGreeting8();

        @CacheResponse(maxEntries = 20_000_000, ttlDuration = 1, ttlUnit = TimeUnit.DAYS)
        Task<String> getGreeting9();

        @CacheResponse(maxEntries = 20_000_000, ttlDuration = 1, ttlUnit = TimeUnit.DAYS)
        Task<String> getGreeting10();

        Task<Void> flushGreeting();
    }

    public static class HelloActor extends AbstractActor implements Hello
    {

        @Override
        public Task<String> getGreeting(final String param)
        {
            return this.getGreeting1();
        }

        @Override
        public Task<String> getGreeting1()
        {
            return Task.fromValue(IdUtils.urlSafeString(32));
        }

        @Override
        public Task<String> getGreeting2()
        {
            return this.getGreeting1();
        }

        @Override
        public Task<String> getGreeting3()
        {
            return this.getGreeting1();
        }

        @Override
        public Task<String> getGreeting4()
        {
            return this.getGreeting1();
        }

        @Override
        public Task<String> getGreeting5()
        {
            return this.getGreeting1();
        }

        @Override
        public Task<String> getGreeting6()
        {
            return this.getGreeting1();
        }

        @Override
        public Task<String> getGreeting7()
        {
            return this.getGreeting1();
        }

        @Override
        public Task<String> getGreeting8()
        {
            return this.getGreeting1();
        }

        @Override
        public Task<String> getGreeting9()
        {
            return this.getGreeting1();
        }

        @Override
        public Task<String> getGreeting10()
        {
            return this.getGreeting1();
        }

        @Override
        public Task<Void> flushGreeting()
        {
            return ExecutionCacheFlushManager.flushAll(this);
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
        this.warmCache = true;
    }

    @TearDown
    public void tearDown()
    {
        this.stage.stop().join();
    }

    @Benchmark
    public void _0_flushThroughput10(final CacheResponseFlushBenchmark state)
    {
        this.runBenchmark(state, 10);
    }

    @Benchmark
    public void _1_flushThroughput100(final CacheResponseFlushBenchmark state)
    {
        this.runBenchmark(state, 100);
    }

    @Benchmark
    public void _2_flushThroughput500(final CacheResponseFlushBenchmark state)
    {
        this.runBenchmark(state, 500);
    }

    @Benchmark
    public void _3_flushThroughput1000(final CacheResponseFlushBenchmark state)
    {
        this.runBenchmark(state, 1_000);
    }

    @Benchmark
    public void _4_flushThroughput10000(final CacheResponseFlushBenchmark state)
    {
        this.runBenchmark(state, 10_000);
    }

    @Benchmark
    public void _5_flushThroughput100000(final CacheResponseFlushBenchmark state)
    {
        this.runBenchmark(state, 100_000);
    }

    private void runBenchmark(final CacheResponseFlushBenchmark state, final int numActors)
    {
        maybePrewarmCache(state, numActors);

        Actor.getReference(Hello.class, Objects.toString(state.actorId))
                .flushGreeting().join();
        this.warmCacheForActor(Objects.toString(state.actorId));

        state.actorId++;
        if (state.actorId >= numActors)
        {
            state.actorId = 0;
        }

    }

    private void maybePrewarmCache(final CacheResponseFlushBenchmark state, final int numActors)
    {
        if (state.warmCache)
        {
            this.preWarmCache(numActors);
            state.warmCache = false;
        }
    }

    private void preWarmCache(final int numActors)
    {
        System.out.println("Warming cache with " + numActors + "x" + NUM_PARAMS);
        IntStream.range(0, numActors).parallel().forEach(i ->
                this.warmCacheForActor(Objects.toString(i)));
    }

    private void warmCacheForActor(final String actorId)
    {
        final Hello actor = Actor.getReference(Hello.class,
                Objects.toString(actorId));

        final ArrayList<Task> tasks = new ArrayList<>();

        tasks.add(actor.getGreeting1());
        tasks.add(actor.getGreeting2());
        tasks.add(actor.getGreeting3());
        tasks.add(actor.getGreeting4());
        tasks.add(actor.getGreeting5());
        tasks.add(actor.getGreeting6());
        tasks.add(actor.getGreeting7());
        tasks.add(actor.getGreeting8());
        tasks.add(actor.getGreeting9());
        tasks.add(actor.getGreeting10());

        IntStream.range(0, NUM_PARAMS).forEach(j ->
                tasks.add(actor.getGreeting(Objects.toString(j))));

        Task.allOf(tasks).join();

    }

}
