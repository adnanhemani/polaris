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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Event listener that buffers in memory and then dumps to persistence. */
@ApplicationScoped
@Identifier("persistence-file-buffer")
public class FileBufferPolarisEventListener extends PersistencePolarisEventListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileBufferPolarisEventListener.class);
    MetaStoreManagerFactory metaStoreManagerFactory;
    PolarisConfigurationStore polarisConfigurationStore;

    // Key: str - Realm
    // Value:
    //    Key: int - shard number
    //    Value: BufferShard - an object representing the directory and file where events are persisted on the filesystem
    private final HashMap<String, HashMap<Integer, BufferShard>> buffers = new HashMap<>();
    ConcurrentLinkedQueue<Runnable> workerQueue = new ConcurrentLinkedQueue<>();
    ConcurrentHashMap<String, FileFlushTask> ongoingFlushTasks = new ConcurrentHashMap<>();

    ExecutorService threadPool;
    private static int shardCount;
    private final int timeToFlush;
    private final Kryo kryo = new Kryo();


    @Inject
    public FileBufferPolarisEventListener(
            MetaStoreManagerFactory metaStoreManagerFactory,
            PolarisConfigurationStore polarisConfigurationStore
    ) {
        this.metaStoreManagerFactory = metaStoreManagerFactory;
        this.polarisConfigurationStore = polarisConfigurationStore;
        this.timeToFlush = polarisConfigurationStore.getConfiguration(null, FeatureConfiguration.EVENT_BUFFER_TIME_TO_FLUSH_IN_MS);
        shardCount = polarisConfigurationStore.getConfiguration(null, FeatureConfiguration.EVENT_BUFFER_NUM_SHARDS);
        int numThreads = polarisConfigurationStore.getConfiguration(null, FeatureConfiguration.EVENT_BUFFER_NUM_THREADS);
        threadPool = Executors.newFixedThreadPool(numThreads);
        kryo.register(PolarisEvent.class);
        kryo.register(PolarisEvent.ResourceType.class);

        // Start all threads
        for (int i = 0; i < numThreads; i++) {
            var unused = threadPool.submit(this::worker);
        }

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
            workerQueue.add(new BufferListingTask(i, realmId));
        }
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
        return System.getProperty("java.io.tmpdir") + "/" + realmId + "/polaris-event-buffer-shard-" + shardNum + "/";
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
            LOGGER.info("Discovered orphaned event buffer potential realm: {}", realmFile.getName());
            File[] realmBufferFiles = realmFile.listFiles();
            if (realmBufferFiles != null) {
                for (File shardDir : realmBufferFiles) {
                    if (!shardDir.getName().startsWith("polaris-event-buffer-shard-")) {
                        continue;
                    }
                    for (File bufferFile : shardDir.listFiles()) {
                        LOGGER.info("Queued file flush for orphaned event buffer file: {}", bufferFile.getAbsolutePath());
                        workerQueue.add(new FileFlushTask(bufferFile.getAbsolutePath(), realmFile.getName()));
                    }
                }
            }
        }
    }

    // Worker method: continuously polls and executes tasks
    private void worker() {
        while (!Thread.currentThread().isInterrupted()) {
            Runnable task = workerQueue.poll();
            if (task != null) {
                try {
                    task.run();
                } catch (Exception e) {
                    LOGGER.error("Error while executing task ", e);
                    if (task instanceof BufferListingTask) {
                        LOGGER.error("Buffer listing task was unable to be executed, re-adding to queue: {}", task);
                        workerQueue.add(task);
                    }
                }
            } else {
                try {
                    // Sleep briefly if no task to avoid busy-waiting
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // Restore interrupt status and exit
                }
            }
        }
    }

    @SuppressWarnings("ClassCanBeStatic")
    private class BufferShard {
        public String directoryLocation;
        public File shardFile;
        public Output output;

        public BufferShard(String directoryLocation, File shardFile, Output output) {
            this.directoryLocation = directoryLocation;
            this.shardFile = shardFile;
            this.output = output;
        }
    }

    class BufferListingTask implements Runnable {
        private final int bufferNum;
        private final String realmId;

        public BufferListingTask(int bufferNum, String realmId) {
            this.bufferNum = bufferNum;
            this.realmId = realmId;
        }

        @Override
        public void run() {
            LOGGER.trace("Starting buffer listing task #{} for realm #{}", bufferNum, realmId);
            ArrayList<String> filesToFlush = new ArrayList<>();
            File dir = new File(getBufferDirectory(realmId, bufferNum));
            File[] bufferFiles = dir.listFiles();
            if (bufferFiles != null) {
                // Get all files except for the current one
                String currentFile = Arrays.stream(bufferFiles).map(File::getName).max(String::compareTo).orElse(null);
                Arrays.stream(bufferFiles).filter(file -> !file.getName().equals(currentFile) && !ongoingFlushTasks.containsKey(file.getAbsolutePath())).map(File::getAbsolutePath).forEach(filesToFlush::add);

                String[] fileDeconstructed = currentFile.split("-");
                if (System.currentTimeMillis() - Long.parseLong(fileDeconstructed[fileDeconstructed.length - 1]) > timeToFlush) {
                    // Create a new shard (file and writer)
                    buffers.get(realmId).put(bufferNum, createShard(realmId, bufferNum));
                }
            }
            for (String file : filesToFlush) {
                workerQueue.add(new FileFlushTask(file, realmId));
            }
            workerQueue.add(new BufferListingTask(bufferNum, realmId));
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
            ongoingFlushTasks.put(this.file, this);
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
                    ongoingFlushTasks.remove(this.file);
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
                workerQueue.add(new FileFlushTask(this.file, realmId));
                ongoingFlushTasks.remove(this.file);
                return;
            }

            // Delete file
            File file = new File(this.file);
            file.delete();
            ongoingFlushTasks.remove(this.file);
        }
    }
}
