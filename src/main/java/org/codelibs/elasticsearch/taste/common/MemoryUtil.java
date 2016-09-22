/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.codelibs.elasticsearch.taste.common;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Memory utilities.
 */
public final class MemoryUtil {

    private static final Logger log = LoggerFactory.getLogger(MemoryUtil.class);

    private MemoryUtil() {
    }

    /**
     * Logs current heap memory statistics.
     *
     * @see Runtime
     */
    public static void logMemoryStatistics() {
        final Runtime runtime = Runtime.getRuntime();
        final long freeBytes = runtime.freeMemory();
        final long maxBytes = runtime.maxMemory();
        final long totalBytes = runtime.totalMemory();
        final long usedBytes = totalBytes - freeBytes;
        log.info("Memory (bytes): {} used, {} heap, {} max", usedBytes,
                totalBytes, maxBytes);
    }

    private static volatile ScheduledExecutorService scheduler;

    /**
     * Constructs and starts a memory logger thread.
     *
     * @param rateInMillis how often memory info should be logged.
     */
    public static void startMemoryLogger(final long rateInMillis) {
        stopMemoryLogger();
        scheduler = Executors.newScheduledThreadPool(1, new ThreadFactory() {
            private final ThreadFactory delegate = Executors
                    .defaultThreadFactory();

            @Override
            public Thread newThread(final Runnable r) {
                final Thread t = delegate.newThread(r);
                t.setDaemon(true);
                return t;
            }
        });
        final Runnable memoryLoogerRunnable = () -> logMemoryStatistics();
        scheduler.scheduleAtFixedRate(memoryLoogerRunnable, rateInMillis,
                rateInMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Constructs and starts a memory logger thread with a logging rate of 1000 milliseconds.
     */
    public static void startMemoryLogger() {
        startMemoryLogger(1000);
    }

    /**
     * Stops the memory logger, if any, started via {@link #startMemoryLogger(long)} or
     * {@link #startMemoryLogger()}.
     */
    public static void stopMemoryLogger() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
    }

}
