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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Per-dataset outcome of the ingest authorization pass for one event, plus the aggregates the
 * adapter reports to the caller.
 *
 * <p>Only one aggregate count leaves the server, and it does not say why a dataset was omitted. A
 * dataset that does not exist and one the caller may not touch produce the same answer: a
 * per-dataset or per-reason answer would make the endpoint a membership oracle over the catalog
 * namespace, with a caller walking candidate table names and reading existence off the response.
 */
public final class LineageAuthorizationResult {

  /** What the authorization pass decided about one dataset occurrence. */
  public enum Disposition {
    /** Names a Polaris entity, resolved, and the caller holds the required privilege. Kept. */
    AUTHORIZED,

    /**
     * Names no catalog of this server, whether because the event names none or because the catalog
     * it names is not in the realm. Kept without a privilege check of its own — it has no securable
     * to check against, and rides the authorized Polaris entity at the other end of its edge.
     */
    EXTERNAL,

    /**
     * Claims a Polaris entity that does not exist, is not addressable, or that the caller lacks the
     * required privilege on. Dropped. These are deliberately one outcome: see the class javadoc.
     */
    OMITTED;

    /** Whether a dataset with this disposition may be forwarded to the ingest provider. */
    boolean isRetained() {
      return this == AUTHORIZED || this == EXTERNAL;
    }
  }

  private final Map<LineageDatasetKey, Disposition> dispositions;
  private final boolean eventEnumerable;

  private LineageAuthorizationResult(
      Map<LineageDatasetKey, Disposition> dispositions, boolean eventEnumerable) {
    this.dispositions = Collections.unmodifiableMap(new LinkedHashMap<>(dispositions));
    this.eventEnumerable = eventEnumerable;
  }

  static LineageAuthorizationResult of(Map<LineageDatasetKey, Disposition> dispositions) {
    return new LineageAuthorizationResult(dispositions, true);
  }

  /**
   * The result for an event whose shape could not be enumerated. Nothing about it can be
   * authorized, so nothing about it may be ingested.
   */
  static LineageAuthorizationResult notEnumerable() {
    return new LineageAuthorizationResult(Map.of(), false);
  }

  /** The per-dataset dispositions, in the order the datasets were encountered in the event. */
  public Map<LineageDatasetKey, Disposition> dispositions() {
    return dispositions;
  }

  /** Whether {@code dataset} may be forwarded to the ingest provider. */
  public boolean isRetained(LineageDatasetKey dataset) {
    Disposition disposition = dispositions.get(dataset);
    return disposition != null && disposition.isRetained();
  }

  /**
   * Whether the event must be ingested as a no-op: accepted, but with nothing handed to the
   * provider.
   *
   * <p>True when no dataset was authorized but at least one was present, which folds the all-denied
   * and all-external cases into one branch, and true for an event whose shape could not be
   * enumerated.
   *
   * <p>False for an event that referenced no datasets at all. Such an event makes no claim about
   * any Polaris entity — there is no edge to forge — and it is ordinary job-lifecycle traffic that
   * an engine emits before any dataset is known, so silently discarding it would lose real signal
   * for no security gain.
   */
  public boolean isNoOp() {
    return !eventEnumerable || (!dispositions.isEmpty() && count(Disposition.AUTHORIZED) == 0);
  }

  /**
   * Whether at least one dataset was dropped, so the event must be rebuilt before it is forwarded.
   * When false the original event object can be passed through untouched.
   */
  public boolean requiresFiltering() {
    return dispositions.values().stream().anyMatch(disposition -> !disposition.isRetained());
  }

  public int authorizedCount() {
    return count(Disposition.AUTHORIZED);
  }

  public int externalCount() {
    return count(Disposition.EXTERNAL);
  }

  /** Datasets omitted, for any reason. */
  public int omittedCount() {
    return count(Disposition.OMITTED);
  }

  private int count(Disposition disposition) {
    return (int) dispositions.values().stream().filter(d -> d == disposition).count();
  }
}
