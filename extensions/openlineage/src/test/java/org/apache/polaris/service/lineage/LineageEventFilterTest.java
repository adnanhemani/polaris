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

import static org.apache.polaris.service.lineage.LineageTestEvents.datasetEvent;
import static org.apache.polaris.service.lineage.LineageTestEvents.input;
import static org.apache.polaris.service.lineage.LineageTestEvents.jobEvent;
import static org.apache.polaris.service.lineage.LineageTestEvents.output;
import static org.apache.polaris.service.lineage.LineageTestEvents.runEvent;
import static org.apache.polaris.service.lineage.LineageTestEvents.staticDataset;
import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.server.OpenLineage;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.polaris.service.lineage.LineageAuthorizationResult.Disposition;
import org.apache.polaris.service.lineage.LineageDatasetKey.Role;
import org.junit.jupiter.api.Test;

/** Tests for {@link LineageEventFilter}. */
class LineageEventFilterTest {

  private static final String NS = "s3://warehouse";

  @Test
  void dropsUnauthorizedInputsAndKeepsAuthorizedOnes() {
    OpenLineage.RunEvent event =
        runEvent(
            List.of(input(NS, "cat.ns.ok"), input(NS, "cat.ns.secret")),
            List.of(output(NS, "cat.ns.dst")));
    LineageAuthorizationResult result =
        result(
            Map.of(
                new LineageDatasetKey(Role.INPUT, NS, "cat.ns.ok"), Disposition.AUTHORIZED,
                new LineageDatasetKey(Role.INPUT, NS, "cat.ns.secret"),
                    Disposition.DROPPED_UNAUTHORIZED,
                new LineageDatasetKey(Role.OUTPUT, NS, "cat.ns.dst"), Disposition.AUTHORIZED));

    OpenLineage.RunEvent filtered =
        (OpenLineage.RunEvent) LineageEventFilter.retainAuthorized(event, result);

    assertThat(filtered.getInputs())
        .extracting(OpenLineage.Dataset::getName)
        .containsExactly("cat.ns.ok");
    assertThat(filtered.getOutputs())
        .extracting(OpenLineage.Dataset::getName)
        .containsExactly("cat.ns.dst");
  }

  @Test
  void preservesEveryOtherRunEventFieldWhenRebuilding() {
    OpenLineage.RunEvent event =
        runEvent(List.of(input(NS, "cat.ns.gone")), List.of(output(NS, "cat.ns.dst")));
    LineageAuthorizationResult result =
        result(
            Map.of(
                new LineageDatasetKey(Role.INPUT, NS, "cat.ns.gone"),
                    Disposition.DROPPED_UNRESOLVED,
                new LineageDatasetKey(Role.OUTPUT, NS, "cat.ns.dst"), Disposition.AUTHORIZED));

    OpenLineage.RunEvent filtered =
        (OpenLineage.RunEvent) LineageEventFilter.retainAuthorized(event, result);

    assertThat(filtered.getEventTime()).isEqualTo(event.getEventTime());
    assertThat(filtered.getProducer()).isEqualTo(event.getProducer());
    assertThat(filtered.getSchemaURL()).isEqualTo(event.getSchemaURL());
    assertThat(filtered.getEventType()).isEqualTo(event.getEventType());
    assertThat(filtered.getRun()).isEqualTo(event.getRun());
    assertThat(filtered.getJob()).isEqualTo(event.getJob());
  }

  @Test
  void rebuildsAJobEventTheSameWay() {
    OpenLineage.JobEvent event =
        jobEvent(List.of(input(NS, "cat.ns.gone")), List.of(output(NS, "cat.ns.dst")));
    LineageAuthorizationResult result =
        result(
            Map.of(
                new LineageDatasetKey(Role.INPUT, NS, "cat.ns.gone"),
                    Disposition.DROPPED_UNRESOLVED,
                new LineageDatasetKey(Role.OUTPUT, NS, "cat.ns.dst"), Disposition.AUTHORIZED));

    OpenLineage.JobEvent filtered =
        (OpenLineage.JobEvent) LineageEventFilter.retainAuthorized(event, result);

    assertThat(filtered.getInputs()).isEmpty();
    assertThat(filtered.getOutputs())
        .extracting(OpenLineage.Dataset::getName)
        .containsExactly("cat.ns.dst");
    assertThat(filtered.getEventTime()).isEqualTo(event.getEventTime());
    assertThat(filtered.getSchemaURL()).isEqualTo(event.getSchemaURL());
  }

  @Test
  void returnsTheSameObjectWhenNothingWasDropped() {
    // Avoids a pointless copy and, more importantly, preserves additionalProperties, which no
    // public constructor can carry over.
    OpenLineage.RunEvent event =
        runEvent(List.of(input(NS, "kafka_topic")), List.of(output(NS, "cat.ns.dst")));
    LineageAuthorizationResult result =
        result(
            Map.of(
                new LineageDatasetKey(Role.INPUT, NS, "kafka_topic"), Disposition.EXTERNAL,
                new LineageDatasetKey(Role.OUTPUT, NS, "cat.ns.dst"), Disposition.AUTHORIZED));

    assertThat(LineageEventFilter.retainAuthorized(event, result)).isSameAs(event);
  }

  @Test
  void retainsExternalDatasetsAlongsideAuthorizedOnes() {
    OpenLineage.RunEvent event =
        runEvent(
            List.of(input("kafka://broker", "topic"), input(NS, "cat.ns.secret")),
            List.of(output(NS, "cat.ns.dst")));
    LineageAuthorizationResult result =
        result(
            Map.of(
                new LineageDatasetKey(Role.INPUT, "kafka://broker", "topic"), Disposition.EXTERNAL,
                new LineageDatasetKey(Role.INPUT, NS, "cat.ns.secret"),
                    Disposition.DROPPED_UNAUTHORIZED,
                new LineageDatasetKey(Role.OUTPUT, NS, "cat.ns.dst"), Disposition.AUTHORIZED));

    OpenLineage.RunEvent filtered =
        (OpenLineage.RunEvent) LineageEventFilter.retainAuthorized(event, result);

    assertThat(filtered.getInputs())
        .extracting(OpenLineage.Dataset::getName)
        .containsExactly("topic");
  }

  @Test
  void keepsANullDatasetListNull() {
    // An event that omitted the field must be rebuilt still having omitted it, rather than with an
    // empty list.
    OpenLineage.RunEvent event =
        runEvent(null, List.of(output(NS, "cat.ns.dst"), output(NS, "cat.ns.no")));
    LineageAuthorizationResult result =
        result(
            Map.of(
                new LineageDatasetKey(Role.OUTPUT, NS, "cat.ns.dst"), Disposition.AUTHORIZED,
                new LineageDatasetKey(Role.OUTPUT, NS, "cat.ns.no"),
                    Disposition.DROPPED_UNAUTHORIZED));

    OpenLineage.RunEvent filtered =
        (OpenLineage.RunEvent) LineageEventFilter.retainAuthorized(event, result);

    assertThat(filtered.getInputs()).isNull();
    assertThat(filtered.getOutputs())
        .extracting(OpenLineage.Dataset::getName)
        .containsExactly("cat.ns.dst");
  }

  @Test
  void aDatasetEventWithItsOneDatasetRetainedIsReturnedUnchanged() {
    OpenLineage.DatasetEvent event = datasetEvent(staticDataset(NS, "cat.ns.tbl"));
    LineageAuthorizationResult result =
        result(
            Map.of(
                new LineageDatasetKey(Role.STANDALONE, NS, "cat.ns.tbl"), Disposition.AUTHORIZED));

    assertThat(LineageEventFilter.retainAuthorized(event, result)).isSameAs(event);
  }

  private static LineageAuthorizationResult result(
      Map<LineageDatasetKey, Disposition> dispositions) {
    return LineageAuthorizationResult.of(new LinkedHashMap<>(dispositions));
  }
}
