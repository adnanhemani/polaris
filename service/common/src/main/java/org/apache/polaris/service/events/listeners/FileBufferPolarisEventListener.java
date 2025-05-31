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

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.AsyncTaskType;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisEvent;
import org.apache.polaris.core.entity.PolarisTaskConstants;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.service.task.TaskExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

/** Event listener that buffers in memory and then dumps to persistence. */
@ApplicationScoped
@Identifier("persistence-file-buffer")
public class FileBufferPolarisEventListener extends PersistencePolarisEventListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileBufferPolarisEventListener.class);
    MetaStoreManagerFactory metaStoreManagerFactory;
    PolarisConfigurationStore polarisConfigurationStore;
    TaskExecutor taskExecutor;

    // Value is a Pair of the first PolarisEvent that is written to the file
    // (used for checking the first event's timestamp on this shard) and the file handle itself
    private final HashMap<Integer, BufferShard> buffers = new HashMap<>();
    private static final int bufferShardCount = 10;
    private final int timeToFlush;


    @Inject
    public FileBufferPolarisEventListener(
            MetaStoreManagerFactory metaStoreManagerFactory,
            PolarisConfigurationStore polarisConfigurationStore,
            TaskExecutor taskExecutor
    ) {
        this.metaStoreManagerFactory = metaStoreManagerFactory;
        this.polarisConfigurationStore = polarisConfigurationStore;
        this.taskExecutor = taskExecutor;
        this.timeToFlush = polarisConfigurationStore.getConfiguration(null, FeatureConfiguration.EVENT_BUFFER_TIME_TO_FLUSH_IN_MS);

        for (int i = 0; i < bufferShardCount; i++) {
            buffers.put(i, createShard(i));
        }
    }

    @Override
    void addToBuffer(PolarisEvent polarisEvent, CallContext callCtx) {
        long ingestionTime = System.currentTimeMillis();
        int shardNum = polarisEvent.hashCode() % bufferShardCount;
        BufferShard bufferShard = buffers.get(shardNum);
        if (bufferShard == null) {
            LOGGER.error("No buffer shard found for shard #{}. Event dropped: {}", shardNum, polarisEvent);
            return;
        }
        if (bufferShard.polarisEvent == null) {
            bufferShard.polarisEvent = polarisEvent;
        }
        try {
            ObjectOutputStream oos = bufferShard.oos;
            oos.writeObject(polarisEvent);
            oos.flush();
            oos.reset();
        } catch (IOException e) {
            LOGGER.error("Unable to write event: {} to buffer shard: {}", polarisEvent, e);
        }

        if (ingestionTime - bufferShard.polarisEvent.getTimestampMs() > this.timeToFlush) {
            // Create a new shard (file and writer)
            buffers.put(shardNum, createShard(shardNum));
            // Create and fire task to persist the events in the old shard file
            PolarisMetaStoreManager polarisMetaStoreManager = metaStoreManagerFactory.getOrCreateMetaStoreManager(callCtx.getRealmContext());
            PolarisBaseEntity taskEntity =
                    new PolarisEntity.Builder()
                            .setId(polarisMetaStoreManager.generateNewEntityId(callCtx.getPolarisCallContext()).getId())
                            .setCatalogId(0L)
                            .setName("persist_logs_and_cleanup_" + bufferShard.shardFile.getName())
                            .setType(PolarisEntityType.TASK)
                            .setSubType(PolarisEntitySubType.NULL_SUBTYPE)
                            .setCreateTimestamp(System.currentTimeMillis())
                            .build();
            Map<String, String> properties = new HashMap<>();
            properties.put(
                    PolarisTaskConstants.TASK_TYPE,
                    String.valueOf(AsyncTaskType.LOG_PERSISTENCE_AND_CLEANUP.typeCode()));
            properties.put("file", bufferShard.shardFile.getAbsolutePath());
            taskEntity.setPropertiesAsMap(properties);
            EntityResult entityResult = polarisMetaStoreManager.createEntityIfNotExists(callCtx.getPolarisCallContext(), null, taskEntity);
            taskExecutor.addTaskHandlerContext(entityResult.getEntity().getId(), callCtx);
        }
    }

    private BufferShard createShard(int shardNum) {
        File file = new File(System.getProperty("java.io.tmpdir") + "/polaris-event-buffer-shard-" + shardNum + "file_timestamp-" + System.currentTimeMillis());
        try {
            file.createNewFile();
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file, true));
            return new BufferShard(null, file, oos);
        } catch (IOException e) {
            LOGGER.error("Buffer shard {} was unable to initialize: {}", shardNum, e);
        }
        return null;
    }

    @SuppressWarnings("ClassCanBeStatic")
    private class BufferShard {
        public PolarisEvent polarisEvent;
        public File shardFile;
        public ObjectOutputStream oos;

        public BufferShard(PolarisEvent polarisEvent, File shardFile, ObjectOutputStream oos) {
            this.polarisEvent = polarisEvent;
            this.shardFile = shardFile;
            this.oos = oos;
        }
    }
}
