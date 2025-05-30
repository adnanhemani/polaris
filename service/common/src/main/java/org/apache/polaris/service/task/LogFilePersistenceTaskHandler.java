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
package org.apache.polaris.service.task;

import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.AsyncTaskType;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisEvent;
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Table cleanup handler resolves the latest {@link TableMetadata} file for a dropped table and
 * schedules a deletion task for <i>each</i> Snapshot found in the {@link TableMetadata}. Manifest
 * cleanup tasks are scheduled in a batch so tasks should be stored atomically.
 */
public class LogFilePersistenceTaskHandler implements TaskHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(LogFilePersistenceTaskHandler.class);
  private final MetaStoreManagerFactory metaStoreManagerFactory;

  public LogFilePersistenceTaskHandler(MetaStoreManagerFactory metaStoreManagerFactory) {
    this.metaStoreManagerFactory = metaStoreManagerFactory;
  }

  @Override
  public boolean canHandleTask(TaskEntity task) {
    return task.getTaskType() == AsyncTaskType.LOG_PERSISTENCE_AND_CLEANUP && taskContainsFile(task);
  }

  private boolean taskContainsFile(TaskEntity task) {
    return task.getPropertiesAsMap().containsKey("file");
  }

  @Override
  public boolean handleTask(TaskEntity task, CallContext callContext) {
    // Read file, deserialize objects
    String filePath = task.getPropertiesAsMap().get("file");
    List<PolarisEvent> polarisEvents = new ArrayList<>();
    try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(filePath))) {
      while (true) {
        PolarisEvent polarisEvent = (PolarisEvent) in.readObject();
        polarisEvents.add(polarisEvent);
      }
    } catch (EOFException e) {
      // End of file reached
    } catch (ClassNotFoundException | IOException e) {
      LOGGER.error("Failed to read events from file {}", filePath, e);
      return false;
    }

    // Write all events back to the metastore
//    callContext = CallContext.getCurrentContext();
//    callContext = CallContext.copyOf(callContext);
    metaStoreManagerFactory.getOrCreateSessionSupplier(callContext.getRealmContext()).get().writeEvents(polarisEvents);

    // Delete file
    File file = new File(filePath);
    file.delete();
    return true;
  }
}
