/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.service.events.listeners;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.KryoBufferUnderflowException;
import com.esotericsoftware.kryo.io.Output;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEvent;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/** Event listener that buffers in memory and then dumps to persistence. */
@ApplicationScoped
@Identifier("persistence-file-buffer")
public class FileBufferPolarisPersistenceEventListener extends PolarisPersistenceEventListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileBufferPolarisPersistenceEventListener.class);
    MetaStoreManagerFactory metaStoreManagerFactory;
    PolarisConfigurationStore polarisConfigurationStore;

    // Key: str - Realm
    // Value:
    //    Key: int - shard number
    //    Value: BufferShard - an object representing the directory and file where events are persisted on the filesystem
    private final HashMap<String, HashMap<Integer, BufferShard>> buffers = new HashMap<>();
    ConcurrentHashMap<String, Future> activeFlushFutures = new ConcurrentHashMap<>();

    ScheduledExecutorService threadPool;
    private static int shardCount;
    private final int timeToFlush;
    private final Kryo kryo = new Kryo();
    private static final String BUFFER_SHARD_PREFIX = "polaris-event-buffer-shard-";


    @Inject
    public FileBufferPolarisPersistenceEventListener(
            MetaStoreManagerFactory metaStoreManagerFactory,
            PolarisConfigurationStore polarisConfigurationStore
    ) {
        this.metaStoreManagerFactory = metaStoreManagerFactory;
        this.polarisConfigurationStore = polarisConfigurationStore;
        this.timeToFlush = polarisConfigurationStore.getConfiguration(null, FeatureConfiguration.EVENT_BUFFER_TIME_TO_FLUSH_IN_MS);
        shardCount = polarisConfigurationStore.getConfiguration(null, FeatureConfiguration.EVENT_BUFFER_NUM_SHARDS);
        int numThreads = polarisConfigurationStore.getConfiguration(null, FeatureConfiguration.EVENT_BUFFER_NUM_THREADS);
        threadPool = Executors.newScheduledThreadPool(numThreads);
        kryo.register(PolarisEvent.class);
        kryo.register(PolarisEvent.ResourceType.class);

        // Recover and push any orphaned files
        discoverAndPushOrphanedFiles();
    }

    @Override
    void addToBuffer(PolarisEvent polarisEvent, CallContext callCtx) {
        int shardNum = polarisEvent.hashCode() % shardCount;
        String realmId = callCtx.getRealmContext().getRealmIdentifier();
        if (!buffers.containsKey(realmId)) {
            createBuffersForRealm(realmId);
        }
        BufferShard bufferShard = buffers.get(realmId).get(shardNum);
        if (bufferShard == null) {
            LOGGER.error("No buffer shard found for realm: #{}, shard #{}. Event dropped: {}", realmId, shardNum, polarisEvent);
            return;
        }

        kryo.writeObject(bufferShard.output, polarisEvent);
        bufferShard.output.flush();
    }

    private void createBuffersForRealm(String realmId) {
        HashMap<Integer, BufferShard> bufferShardsForRealm = new HashMap<>();
        buffers.put(realmId, bufferShardsForRealm);
        for (int i = 0; i < shardCount; i++) {
            bufferShardsForRealm.put(i, createShard(realmId, i));
        }
        ScheduledFuture<?> future = threadPool.schedule(new BufferListingTask(shardCount, realmId), timeToFlush, TimeUnit.MILLISECONDS);
    }

    private BufferShard createShard(String realmId, int shardNum) {
        File file = new File(getBufferDirectory(realmId, shardNum) + "buffer_timestamp-" + System.currentTimeMillis());
        File parent = file.getParentFile();
        if (parent != null && !parent.exists()) {
            parent.mkdirs();  // Creates all missing parent directories
        }
        try {
            file.createNewFile();
            Output output = new Output(new FileOutputStream(file));
            return new BufferShard(null, file, output);
        } catch (IOException e) {
            LOGGER.error("Buffer shard {} was unable to initialize: {}", shardNum, e);
        }
        return null;
    }

    private String getBufferDirectory(String realmId, int shardNum) {
        return System.getProperty("java.io.tmpdir") + "/" + realmId + "/" + BUFFER_SHARD_PREFIX + shardNum + "/";
    }

    private RealmContext getRealmContext(String realmId) {
        return () -> realmId;
    }

    private void discoverAndPushOrphanedFiles() {
        LOGGER.info("Attempting to discover orphaned event buffer files");
        File topLevelDirectory = new File(System.getProperty("java.io.tmpdir"));
        File[] realmFiles = topLevelDirectory.listFiles();
        if (realmFiles == null || realmFiles.length == 0) {
            LOGGER.info("No discovered orphaned event buffer files in {}", topLevelDirectory.getAbsolutePath());
            return;
        }
        for (File realmFile : realmFiles) {
            LOGGER.info("Discovered a potential orphaned event buffer for realm: {}", realmFile.getName());
            File[] realmBufferFiles = realmFile.listFiles();
            if (realmBufferFiles != null) {
                for (File shardDir : realmBufferFiles) {
                    if (!shardDir.getName().startsWith(BUFFER_SHARD_PREFIX)) {
                        continue;
                    }
                    for (File bufferFile : shardDir.listFiles()) {
                        LOGGER.info("Queued file flush for orphaned event buffer file: {}", bufferFile.getAbsolutePath());
                        FileFlushTask task = new FileFlushTask(bufferFile.getAbsolutePath(), realmFile.getName());
                        Future future = threadPool.submit(task);
                        if (activeFlushFutures.putIfAbsent(bufferFile.getAbsolutePath(), future) != null) {
                            // Other tasks are working on this file, cancel this task
                            future.cancel(false);
                        }
                    }
                }
            }
        }
    }

    private record BufferShard(String directoryLocation, File shardFile, Output output) {}

    class BufferListingTask implements Runnable {
        private final int numBuffers;
        private final String realmId;

        public BufferListingTask(int numBuffers, String realmId) {
            this.numBuffers = numBuffers;
            this.realmId = realmId;
        }

        @Override
        public void run() {
            for (int i = 0; i < numBuffers; i++) {
                try {
                    listRealmBuffer(i);
                } catch (Exception e) {
                    LOGGER.error("Error while flushing buffer realm {}, shard {}", realmId, i, e);
                }
            }
            var future = threadPool.schedule(new BufferListingTask(numBuffers, realmId), timeToFlush, TimeUnit.MILLISECONDS);
        }

        private void listRealmBuffer(int bufferNum) {
            LOGGER.trace("Starting buffer listing task #{} for realm #{}", bufferNum, realmId);
            ArrayList<String> filesToFlush = new ArrayList<>();
            File dir = new File(getBufferDirectory(realmId, bufferNum));
            File[] bufferFiles = dir.listFiles();
            if (bufferFiles != null) {
                // Get all files except for the current one
                String currentFile = Arrays.stream(bufferFiles).map(File::getAbsolutePath).max(String::compareTo).orElse(null);
                Arrays.stream(bufferFiles).map(File::getAbsolutePath).filter(file -> !file.equals(currentFile) && ! activeFlushFutures.containsKey(file)).forEach(filesToFlush::add);

                // Get the buffer start time from the file name
                String[] fileDeconstructed = currentFile.split("-");
                // Check if this buffer has been alive for longer than the `timeToFlush`
                if (System.currentTimeMillis() - Long.parseLong(fileDeconstructed[fileDeconstructed.length - 1]) > timeToFlush) {
                    // Create a new shard (file and writer)
                    buffers.get(realmId).put(bufferNum, createShard(realmId, bufferNum));
                }
            }
            for (String file : filesToFlush) {
                Future future = threadPool.submit(new FileFlushTask(file, realmId));
                if (activeFlushFutures.putIfAbsent(file, future) != null) {
                    // Some other task has been created for this file, cancel this one.
                    future.cancel(true);
                }
            }
            LOGGER.trace("Ended buffer listing task #{} for realm #{}", bufferNum, realmId);
        }
    }

    class FileFlushTask implements Runnable {
        private final String file;
        private final String realmId;

        public FileFlushTask(String file, String realmId) {
            this.file = file;
            this.realmId = realmId;
        }

        @Override
        public void run() {
            LOGGER.trace("Starting file flush task #{}", file);
            List<PolarisEvent> polarisEvents = new ArrayList<>();
            try (Input in = new Input(new FileInputStream(this.file))) {
                while (true) {
                    PolarisEvent polarisEvent = kryo.readObject(in, PolarisEvent.class);
                    polarisEvents.add(polarisEvent);
                }
            } catch (KryoException e) {
                // Possibly end of file reached
                if (!(e.getCause() instanceof EOFException) && !(e instanceof KryoBufferUnderflowException)) {
                    LOGGER.error("Failed to read events from file {}", file, e);
                    activeFlushFutures.remove(this.file);
                    return;
                }
            } catch (IOException e) {
                if (e.getCause() instanceof FileNotFoundException) {
                    LOGGER.trace("Skipping file {}, as it may no longer be present", file);
                }
            }

            try {
                if (!polarisEvents.isEmpty()) {
                    // Write all events back to the metastore
                    metaStoreManagerFactory.getOrCreateSessionSupplier(getRealmContext(this.realmId)).get().writeEvents(polarisEvents);
                }
            } catch (RuntimeException e) {
                LOGGER.error("Failed to write events to meta store", e);
                Future future = threadPool.submit(new FileFlushTask(this.file, realmId));
                activeFlushFutures.put(this.file, future);
                return;
            }

            // Delete file
            File file = new File(this.file);
            file.delete();
            activeFlushFutures.remove(this.file);
        }
    }
}
