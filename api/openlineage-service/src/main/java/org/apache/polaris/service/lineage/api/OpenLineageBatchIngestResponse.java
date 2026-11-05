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
package org.apache.polaris.service.lineage.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * Response body for the OpenLineage batch ingest endpoint ({@code POST
 * /api/openlineage/v1/lineage/batch}).
 *
 * <p>Shape follows the OpenLineage batch API (<a
 * href="https://openlineage.io/apidocs/openapi/">openlineage.io/apidocs/openapi</a>): an overall
 * {@code status}, a {@code summary} of received/successful/failed counts, and a {@code
 * failed_events} array describing each rejected event and whether the failure is retriable.
 */
public final class OpenLineageBatchIngestResponse {

  /** Overall outcome of processing the batch. */
  public enum Status {
    SUCCESS,
    PARTIAL,
    FAILURE
  }

  private final Status status;
  private final Summary summary;
  private final List<FailedEvent> failedEvents;

  public OpenLineageBatchIngestResponse(
      Status status, Summary summary, List<FailedEvent> failedEvents) {
    this.status = status;
    this.summary = summary;
    this.failedEvents = failedEvents;
  }

  @JsonProperty("status")
  public Status status() {
    return status;
  }

  @JsonProperty("summary")
  public Summary summary() {
    return summary;
  }

  @JsonProperty("failed_events")
  public List<FailedEvent> failedEvents() {
    return failedEvents;
  }

  /**
   * Received/successful/failed counts for the batch, plus the counts of dataset references that
   * were omitted from otherwise successful events by ingest authorization.
   *
   * <p>The omitted counts are aggregates over the whole batch and are grouped by reason only. They
   * never identify which dataset was omitted: a per-dataset answer would make the endpoint a
   * membership oracle over the catalog namespace. An event whose datasets were all omitted still
   * counts as {@code successful} — lineage ingest is best-effort by design, and failing an engine's
   * job over a lineage-only authorization gap is worse than recording nothing.
   */
  public static final class Summary {
    private final int received;
    private final int successful;
    private final int failed;
    private final int omittedUnresolved;
    private final int omittedUnauthorized;

    public Summary(int received, int successful, int failed) {
      this(received, successful, failed, 0, 0);
    }

    public Summary(
        int received, int successful, int failed, int omittedUnresolved, int omittedUnauthorized) {
      this.received = received;
      this.successful = successful;
      this.failed = failed;
      this.omittedUnresolved = omittedUnresolved;
      this.omittedUnauthorized = omittedUnauthorized;
    }

    @JsonProperty("received")
    public int received() {
      return received;
    }

    @JsonProperty("successful")
    public int successful() {
      return successful;
    }

    @JsonProperty("failed")
    public int failed() {
      return failed;
    }

    /** Dataset references omitted because they name no entity the caller can see. */
    @JsonProperty("omitted_unresolved")
    public int omittedUnresolved() {
      return omittedUnresolved;
    }

    /** Dataset references omitted because the caller lacks the required lineage privilege. */
    @JsonProperty("omitted_unauthorized")
    public int omittedUnauthorized() {
      return omittedUnauthorized;
    }
  }

  /** Describes a single event in the batch that could not be ingested. */
  public static final class FailedEvent {
    private final int index;
    private final boolean retriable;
    private final String message;

    public FailedEvent(int index, boolean retriable, String message) {
      this.index = index;
      this.retriable = retriable;
      this.message = message;
    }

    /** Zero-based position of the event in the submitted batch. */
    @JsonProperty("index")
    public int index() {
      return index;
    }

    /** Whether re-submitting this event later could succeed. */
    @JsonProperty("retriable")
    public boolean retriable() {
      return retriable;
    }

    @JsonProperty("message")
    public String message() {
      return message;
    }
  }
}
