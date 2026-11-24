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
package org.apache.polaris.service.lineage;

import io.openlineage.server.OpenLineage;
import java.util.List;
import org.apache.polaris.service.lineage.LineageDatasetKey.Role;

/**
 * Rebuilds an OpenLineage event so it carries only the datasets the authorization pass retained.
 *
 * <p>Filtering the event itself, rather than passing the original along with advisory metadata, is
 * what makes the guarantee structural: {@link OpenLineageIngestProvider} is a pluggable SPI, and a
 * provider that ignores every contract Polaris states about what it may persist still cannot write
 * a forged edge, because the dropped dataset is not in the object it was handed.
 *
 * <p>The {@code OpenLineage.*Event} classes are {@code final} but expose public all-args
 * constructors, so an event can be rebuilt field for field. The one field a rebuild cannot carry
 * over is {@code additionalProperties}, which has no public setter; that loss is confined to events
 * that actually had a dataset dropped, since an event with nothing dropped is returned as the very
 * same object.
 */
public final class LineageEventFilter {

  private LineageEventFilter() {}

  /**
   * Returns {@code event} with every dropped dataset removed, or {@code event} itself when nothing
   * was dropped.
   *
   * <p>Must not be called for a result that {@link LineageAuthorizationResult#isNoOp() is a no-op};
   * such an event is never forwarded at all.
   */
  public static OpenLineage.BaseEvent retainAuthorized(
      OpenLineage.BaseEvent event, LineageAuthorizationResult result) {
    if (!result.requiresFiltering()) {
      // Nothing was dropped, so the event already says only what the caller may assert. Returning
      // the original preserves additionalProperties and avoids a pointless copy.
      return event;
    }

    if (event instanceof OpenLineage.RunEvent runEvent) {
      return new OpenLineage.RunEvent(
          runEvent.getEventTime(),
          runEvent.getProducer(),
          runEvent.getSchemaURL(),
          runEvent.getEventType(),
          runEvent.getRun(),
          runEvent.getJob(),
          retain(runEvent.getInputs(), Role.INPUT, result),
          retain(runEvent.getOutputs(), Role.OUTPUT, result));
    }

    if (event instanceof OpenLineage.JobEvent jobEvent) {
      return new OpenLineage.JobEvent(
          jobEvent.getEventTime(),
          jobEvent.getProducer(),
          jobEvent.getSchemaURL(),
          jobEvent.getJob(),
          retain(jobEvent.getInputs(), Role.INPUT, result),
          retain(jobEvent.getOutputs(), Role.OUTPUT, result));
    }

    if (event instanceof OpenLineage.DatasetEvent) {
      // A DatasetEvent carries exactly one dataset. If it had been dropped nothing would be
      // authorized, so the no-op rule would have short-circuited before reaching here; reaching
      // here means the dataset was retained and there is nothing to remove.
      return event;
    }

    throw new IllegalStateException(
        "Cannot filter lineage event of unenumerable shape: "
            + (event == null ? "null" : event.getClass().getName()));
  }

  /**
   * Keeps only the retained datasets of {@code datasets}, preserving a null list as null so an
   * event that omitted the field is rebuilt having still omitted it.
   */
  private static <T extends OpenLineage.Dataset> List<T> retain(
      List<T> datasets, Role role, LineageAuthorizationResult result) {
    if (datasets == null) {
      return null;
    }
    return datasets.stream()
        .filter(dataset -> result.isRetained(LineageDatasetKey.of(role, dataset)))
        .toList();
  }
}
