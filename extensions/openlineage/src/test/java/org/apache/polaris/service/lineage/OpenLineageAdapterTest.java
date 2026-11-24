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

import static org.apache.polaris.core.auth.PolarisAuthorizableOperation.INGEST_TABLE_LINEAGE;
import static org.apache.polaris.core.auth.PolarisAuthorizableOperation.REFERENCE_LINEAGE_INPUT_TABLE;
import static org.apache.polaris.service.lineage.FakeLineageCatalog.namespace;
import static org.apache.polaris.service.lineage.FakeLineageCatalog.sparkInput;
import static org.apache.polaris.service.lineage.FakeLineageCatalog.sparkOutput;
import static org.apache.polaris.service.lineage.FakeLineageCatalog.table;
import static org.apache.polaris.service.lineage.LineageTestEvents.datasetFreeEvent;
import static org.apache.polaris.service.lineage.LineageTestEvents.input;
import static org.apache.polaris.service.lineage.LineageTestEvents.runEvent;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.server.OpenLineage;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.service.lineage.api.OpenLineageBatchIngestResponse;
import org.apache.polaris.service.lineage.api.PolarisLineageEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Unit tests for {@link OpenLineageAdapter}: authorizing an event's dataset references, mapping
 * provider {@link OpenLineageIngestResult}s to HTTP responses, and aggregating a batch of per-event
 * outcomes into an {@link OpenLineageBatchIngestResponse}.
 *
 * <p>The provider is mocked so every outcome branch — including {@code REJECTED} and {@code
 * UNAVAILABLE} — can be exercised. The full-server {@code OpenLineageServiceIT} only ever sees the
 * no-op provider's {@code ACCEPTED}, so the partial/failure aggregation and the non-201 status
 * mappings have no other coverage.
 *
 * <p>Authorization is driven through a real {@link LineageIngestAuthorizer} over a fake catalog
 * (resolvable path keys plus granted {@code (operation, securable)} pairs) rather than a stubbed
 * result, so these tests exercise the end-to-end path the server takes.
 */
class OpenLineageAdapterTest {

  private OpenLineageIngestProvider provider;
  private RealmConfig realmConfig;
  private FakeLineageCatalog catalog;
  private OpenLineageAdapter adapter;

  @BeforeEach
  void setUp() {
    provider = mock(OpenLineageIngestProvider.class);
    realmConfig = mock(RealmConfig.class);
    when(realmConfig.getConfig(FeatureConfiguration.ENABLE_OPENLINEAGE_INGEST)).thenReturn(true);

    catalog = new FakeLineageCatalog();
    adapter = new OpenLineageAdapter(provider, realmConfig, catalog.ingestAuthorizer());
  }

  // ---------------------------------------------------------------------------------------------
  // Response mapping. An event with no dataset references makes no claim about any Polaris entity,
  // so it reaches the provider unconditionally and isolates the mapping logic.
  // ---------------------------------------------------------------------------------------------

  @Nested
  class ResponseMapping {

    @Test
    void acceptedMapsTo201() {
      when(provider.ingest(any())).thenReturn(OpenLineageIngestResult.ACCEPTED);
      assertThat(adapter.sendLineageEvent(event(datasetFreeEvent())).getStatus())
          .isEqualTo(Response.Status.CREATED.getStatusCode());
    }

    @Test
    void rejectedMapsTo400() {
      when(provider.ingest(any())).thenReturn(OpenLineageIngestResult.REJECTED);
      assertThat(adapter.sendLineageEvent(event(datasetFreeEvent())).getStatus())
          .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
    }

    @Test
    void unavailableMapsTo503() {
      when(provider.ingest(any())).thenReturn(OpenLineageIngestResult.UNAVAILABLE);
      assertThat(adapter.sendLineageEvent(event(datasetFreeEvent())).getStatus())
          .isEqualTo(Response.Status.SERVICE_UNAVAILABLE.getStatusCode());
    }

    @Test
    void anEventWithNoDatasetReferencesIsForwardedUnchanged() {
      when(provider.ingest(any())).thenReturn(OpenLineageIngestResult.ACCEPTED);
      OpenLineage.RunEvent raw = datasetFreeEvent();

      adapter.sendLineageEvent(event(raw));

      assertThat(capturedEvent()).isSameAs(raw);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Ingest authorization.
  // ---------------------------------------------------------------------------------------------

  @Nested
  class Authorization {

    @Test
    void anUnreadableInputNeverReachesTheProviderButTheCallStillSucceeds() {
      // THE FORGERY VECTOR, end to end. A caller who can write cat.ns.dst must not be able to
      // record
      // that dst was derived from cat.ns.secret, which they cannot read. The event is still
      // accepted.
      when(provider.ingest(any())).thenReturn(OpenLineageIngestResult.ACCEPTED);
      catalog.tableExists("cat", "ns", "secret");
      catalog.tableExists("cat", "ns", "dst");
      catalog.grant(INGEST_TABLE_LINEAGE, table("cat", "ns", "dst"));

      Response response =
          adapter.sendLineageEvent(
              event(
                  runEvent(
                      List.of(sparkInput("cat", "ns.secret")),
                      List.of(sparkOutput("cat", "ns.dst")))));

      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());

      OpenLineage.RunEvent forwarded = (OpenLineage.RunEvent) capturedEvent();
      assertThat(forwarded.getInputs()).isEmpty();
      assertThat(forwarded.getOutputs())
          .extracting(OpenLineage.Dataset::getName)
          .containsExactly("wh/ns/dst");
      // Nothing in the forwarded event mentions the unreadable table at all, in either direction.
      assertThat(allDatasetNames(forwarded)).doesNotContain("wh/ns/secret");

      assertThat(response.getHeaderString(OpenLineageAdapter.OMITTED_HEADER)).isEqualTo("1");
    }

    @Test
    void anInputWithOnlyReadAccessIsForwarded() {
      // The ordinary ETL case that motivated splitting REFERENCE_LINEAGE_INPUT_TABLE out.
      when(provider.ingest(any())).thenReturn(OpenLineageIngestResult.ACCEPTED);
      catalog.tableExists("cat", "ns", "src");
      catalog.grant(REFERENCE_LINEAGE_INPUT_TABLE, table("cat", "ns", "src"));

      Response response =
          adapter.sendLineageEvent(
              event(runEvent(List.of(sparkInput("cat", "ns.src")), List.of())));

      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
      assertThat(allDatasetNames((OpenLineage.RunEvent) capturedEvent()))
          .containsExactly("wh/ns/src");
    }

    @Test
    void aCreateTableAsSelectOutputIsAuthorizedAgainstItsParentNamespace() {
      when(provider.ingest(any())).thenReturn(OpenLineageIngestResult.ACCEPTED);
      catalog.namespaceExists("cat", "ns");
      catalog.grant(INGEST_TABLE_LINEAGE, namespace("cat", "ns"));

      Response response =
          adapter.sendLineageEvent(
              event(runEvent(List.of(), List.of(sparkOutput("cat", "ns.brand_new")))));

      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
      assertThat(allDatasetNames((OpenLineage.RunEvent) capturedEvent()))
          .containsExactly("wh/ns/brand_new");
    }

    @Test
    void externalDatasetsAreRetainedBesideAnAuthorizedPolarisEntity() {
      when(provider.ingest(any())).thenReturn(OpenLineageIngestResult.ACCEPTED);
      catalog.tableExists("cat", "ns", "dst");
      catalog.grant(INGEST_TABLE_LINEAGE, table("cat", "ns", "dst"));

      adapter.sendLineageEvent(
          event(
              runEvent(
                  List.of(input("kafka://broker", "topic")),
                  List.of(sparkOutput("cat", "ns.dst")))));

      assertThat(allDatasetNames((OpenLineage.RunEvent) capturedEvent()))
          .containsExactlyInAnyOrder("topic", "wh/ns/dst");
    }

    @Test
    void anEventThatAuthorizesNothingSucceedsAsANoOpWithoutCallingTheProvider() {
      catalog.tableExists("cat", "ns", "a");
      catalog.tableExists("cat", "ns", "b");
      // No grants at all: everything is denied.

      Response response =
          adapter.sendLineageEvent(
              event(
                  runEvent(
                      List.of(sparkInput("cat", "ns.a")),
                      List.of(sparkOutput("cat", "ns.b"), sparkOutput("cat", "ns.missing")))));

      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
      verify(provider, never()).ingest(any());
      // One existing input and one missing output are omitted alike, plus the denied output.
      assertThat(response.getHeaderString(OpenLineageAdapter.OMITTED_HEADER)).isEqualTo("3");
    }

    @Test
    void anAllExternalEventSucceedsAsANoOp() {
      Response response =
          adapter.sendLineageEvent(
              event(runEvent(List.of(input("kafka://broker", "topic")), List.of())));

      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
      verify(provider, never()).ingest(any());
      // Nothing was omitted for an authorization reason, so no advisory header is emitted.
      assertThat(response.getHeaderString(OpenLineageAdapter.OMITTED_HEADER)).isNull();
    }

    @Test
    void aDatasetOfACatalogNotInTheRealmIsTreatedAsExternal() {
      Response response =
          adapter.sendLineageEvent(
              event(runEvent(List.of(sparkInput("elsewhere", "ns.src")), List.of())));

      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
      verify(provider, never()).ingest(any());
      assertThat(response.getHeaderString(OpenLineageAdapter.OMITTED_HEADER)).isNull();
    }

    @Test
    void anEventOfUnenumerableShapeSucceedsAsANoOp() {
      Response response = adapter.sendLineageEvent(new PolarisLineageEvent(null));

      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
      verify(provider, never()).ingest(any());
    }

    @Test
    void noOmissionHeaderIsEmittedWhenNothingWasOmitted() {
      when(provider.ingest(any())).thenReturn(OpenLineageIngestResult.ACCEPTED);

      Response response = adapter.sendLineageEvent(event(datasetFreeEvent()));

      assertThat(response.getHeaderString(OpenLineageAdapter.OMITTED_HEADER)).isNull();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Batch aggregation.
  // ---------------------------------------------------------------------------------------------

  @Nested
  class BatchAggregation {

    @Test
    void batchAllAcceptedIsSuccess() {
      when(provider.ingest(any())).thenReturn(OpenLineageIngestResult.ACCEPTED);

      Response response =
          adapter.sendLineageEventBatch(
              List.of(
                  event(datasetFreeEvent()), event(datasetFreeEvent()), event(datasetFreeEvent())));

      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      OpenLineageBatchIngestResponse body = body(response);
      assertThat(body.status()).isEqualTo(OpenLineageBatchIngestResponse.Status.SUCCESS);
      assertThat(body.summary().received()).isEqualTo(3);
      assertThat(body.summary().successful()).isEqualTo(3);
      assertThat(body.summary().failed()).isZero();
      assertThat(body.summary().omitted()).isZero();
      assertThat(body.failedEvents()).isEmpty();
    }

    @Test
    void batchMixedOutcomesIsPartialWithPerEventDetail() {
      // Event 0 accepted, event 1 rejected (not retriable), event 2 backend unavailable
      // (retriable).
      when(provider.ingest(any()))
          .thenReturn(
              OpenLineageIngestResult.ACCEPTED,
              OpenLineageIngestResult.REJECTED,
              OpenLineageIngestResult.UNAVAILABLE);

      Response response =
          adapter.sendLineageEventBatch(
              List.of(
                  event(datasetFreeEvent()), event(datasetFreeEvent()), event(datasetFreeEvent())));

      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      OpenLineageBatchIngestResponse body = body(response);
      assertThat(body.status()).isEqualTo(OpenLineageBatchIngestResponse.Status.PARTIAL);
      assertThat(body.summary().received()).isEqualTo(3);
      assertThat(body.summary().successful()).isEqualTo(1);
      assertThat(body.summary().failed()).isEqualTo(2);

      assertThat(body.failedEvents()).hasSize(2);
      OpenLineageBatchIngestResponse.FailedEvent rejected = body.failedEvents().get(0);
      assertThat(rejected.index()).isEqualTo(1);
      assertThat(rejected.retriable()).isFalse();
      assertThat(rejected.message()).isEqualTo("Event rejected");

      OpenLineageBatchIngestResponse.FailedEvent unavailable = body.failedEvents().get(1);
      assertThat(unavailable.index()).isEqualTo(2);
      assertThat(unavailable.retriable()).isTrue();
      assertThat(unavailable.message()).isEqualTo("Ingest backend unavailable");
    }

    @Test
    void batchAllFailedIsFailure() {
      when(provider.ingest(any()))
          .thenReturn(OpenLineageIngestResult.REJECTED, OpenLineageIngestResult.UNAVAILABLE);

      Response response =
          adapter.sendLineageEventBatch(
              List.of(event(datasetFreeEvent()), event(datasetFreeEvent())));

      OpenLineageBatchIngestResponse body = body(response);
      assertThat(body.status()).isEqualTo(OpenLineageBatchIngestResponse.Status.FAILURE);
      assertThat(body.summary().received()).isEqualTo(2);
      assertThat(body.summary().successful()).isZero();
      assertThat(body.summary().failed()).isEqualTo(2);
      assertThat(body.failedEvents()).hasSize(2);
    }

    @Test
    void emptyBatchIsSuccessAndDoesNotCallProvider() {
      Response response = adapter.sendLineageEventBatch(List.of());

      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      OpenLineageBatchIngestResponse body = body(response);
      assertThat(body.status()).isEqualTo(OpenLineageBatchIngestResponse.Status.SUCCESS);
      assertThat(body.summary().received()).isZero();
      assertThat(body.summary().successful()).isZero();
      assertThat(body.summary().failed()).isZero();
      assertThat(body.failedEvents()).isEmpty();
      verify(provider, never()).ingest(any());
    }

    @Test
    void anEventThatAuthorizesNothingCountsAsSuccessfulWithItsOmissionsInTheSummary() {
      when(provider.ingest(any())).thenReturn(OpenLineageIngestResult.ACCEPTED);
      catalog.tableExists("cat", "ns", "denied");

      Response response =
          adapter.sendLineageEventBatch(
              List.of(
                  event(datasetFreeEvent()),
                  // Authorizes nothing: one existing-but-denied input, one nonexistent input.
                  event(
                      runEvent(
                          List.of(sparkInput("cat", "ns.denied"), sparkInput("cat", "ns.missing")),
                          List.of()))));

      OpenLineageBatchIngestResponse body = body(response);
      assertThat(body.status()).isEqualTo(OpenLineageBatchIngestResponse.Status.SUCCESS);
      assertThat(body.summary().received()).isEqualTo(2);
      assertThat(body.summary().successful()).isEqualTo(2);
      assertThat(body.summary().failed()).isZero();
      assertThat(body.summary().omitted()).isEqualTo(2);
      assertThat(body.failedEvents()).isEmpty();
      // Only the dataset-free event had anything to forward.
      verify(provider).ingest(any());
    }

    @Test
    void aThrowingEventBecomesAFailedEntryWithoutAbortingTheBatch() {
      // The provider is a pluggable SPI and the Iceberg exception mapper is registered globally
      // over
      // /api/openlineage/v1, so an escaping RuntimeException would otherwise collapse the whole
      // batch
      // into one error response and lose every other event's outcome.
      when(provider.ingest(any()))
          .thenReturn(OpenLineageIngestResult.ACCEPTED)
          .thenThrow(new IllegalStateException("provider blew up"))
          .thenReturn(OpenLineageIngestResult.ACCEPTED);

      Response response =
          adapter.sendLineageEventBatch(
              List.of(
                  event(datasetFreeEvent()), event(datasetFreeEvent()), event(datasetFreeEvent())));

      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      OpenLineageBatchIngestResponse body = body(response);
      assertThat(body.status()).isEqualTo(OpenLineageBatchIngestResponse.Status.PARTIAL);
      assertThat(body.summary().received()).isEqualTo(3);
      assertThat(body.summary().successful()).isEqualTo(2);
      assertThat(body.summary().failed()).isEqualTo(1);

      assertThat(body.failedEvents()).hasSize(1);
      OpenLineageBatchIngestResponse.FailedEvent failure = body.failedEvents().get(0);
      assertThat(failure.index()).isEqualTo(1);
      assertThat(failure.retriable()).isFalse();
      // The message must not leak the underlying exception to the caller.
      assertThat(failure.message()).isEqualTo("Event could not be processed");
      assertThat(failure.message()).doesNotContain("provider blew up");
    }

    @Test
    void aBatchWhereEveryEventThrowsIsAFailure() {
      when(provider.ingest(any())).thenThrow(new IllegalStateException("provider blew up"));

      Response response =
          adapter.sendLineageEventBatch(
              List.of(event(datasetFreeEvent()), event(datasetFreeEvent())));

      OpenLineageBatchIngestResponse body = body(response);
      assertThat(body.status()).isEqualTo(OpenLineageBatchIngestResponse.Status.FAILURE);
      assertThat(body.summary().successful()).isZero();
      assertThat(body.summary().failed()).isEqualTo(2);
    }

    @Test
    void aSingleEventThrowIsNotSwallowed() {
      // Fault isolation is deliberately batch-only. The single-event endpoint has no sibling
      // outcomes to protect, so an unexpected throw must keep propagating to the exception mapper
      // rather than be quietly reported as a success.
      when(provider.ingest(any())).thenThrow(new IllegalStateException("provider blew up"));

      assertThatThrownBy(() -> adapter.sendLineageEvent(event(datasetFreeEvent())))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("provider blew up");
    }

    @Test
    void theOmittedCountAccumulatesAcrossTheBatch() {
      when(provider.ingest(any())).thenReturn(OpenLineageIngestResult.ACCEPTED);
      catalog.tableExists("cat", "ns", "dst");
      catalog.tableExists("cat", "ns", "denied");
      catalog.grant(INGEST_TABLE_LINEAGE, table("cat", "ns", "dst"));

      PolarisLineageEvent partiallyAuthorized =
          event(
              runEvent(
                  List.of(sparkInput("cat", "ns.denied"), sparkInput("cat", "ns.missing")),
                  List.of(sparkOutput("cat", "ns.dst"))));

      Response response =
          adapter.sendLineageEventBatch(List.of(partiallyAuthorized, partiallyAuthorized));

      OpenLineageBatchIngestResponse body = body(response);
      assertThat(body.status()).isEqualTo(OpenLineageBatchIngestResponse.Status.SUCCESS);
      assertThat(body.summary().successful()).isEqualTo(2);
      assertThat(body.summary().omitted()).isEqualTo(4);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Feature flag. Checked before anything else, so no authorization or ingest happens.
  // ---------------------------------------------------------------------------------------------

  @Nested
  class FeatureFlag {

    @Test
    void disabledFlagReturns501AndDoesNotCallProvider() {
      when(realmConfig.getConfig(FeatureConfiguration.ENABLE_OPENLINEAGE_INGEST)).thenReturn(false);

      Response response = adapter.sendLineageEvent(event(datasetFreeEvent()));

      assertThat(response.getStatus()).isEqualTo(Response.Status.NOT_IMPLEMENTED.getStatusCode());
      verify(provider, never()).ingest(any());
    }

    @Test
    void disabledFlagBatchReturns501AndDoesNotCallProvider() {
      when(realmConfig.getConfig(FeatureConfiguration.ENABLE_OPENLINEAGE_INGEST)).thenReturn(false);

      Response response =
          adapter.sendLineageEventBatch(
              List.of(event(datasetFreeEvent()), event(datasetFreeEvent())));

      assertThat(response.getStatus()).isEqualTo(Response.Status.NOT_IMPLEMENTED.getStatusCode());
      verify(provider, never()).ingest(any());
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Fixture.
  // ---------------------------------------------------------------------------------------------

  private static PolarisLineageEvent event(OpenLineage.BaseEvent event) {
    return new PolarisLineageEvent(event);
  }

  private static OpenLineageBatchIngestResponse body(Response response) {
    return (OpenLineageBatchIngestResponse) response.getEntity();
  }

  private OpenLineage.BaseEvent capturedEvent() {
    ArgumentCaptor<OpenLineageIngestRequest> captor =
        ArgumentCaptor.forClass(OpenLineageIngestRequest.class);
    verify(provider).ingest(captor.capture());
    return captor.getValue().event();
  }

  private static List<String> allDatasetNames(OpenLineage.RunEvent event) {
    List<String> names = new ArrayList<>();
    if (event.getInputs() != null) {
      event.getInputs().forEach(dataset -> names.add(dataset.getName()));
    }
    if (event.getOutputs() != null) {
      event.getOutputs().forEach(dataset -> names.add(dataset.getName()));
    }
    return names;
  }
}
