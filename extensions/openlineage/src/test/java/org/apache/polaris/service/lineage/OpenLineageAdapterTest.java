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

import static org.apache.polaris.core.auth.PolarisAuthorizableOperation.INGEST_LINEAGE;
import static org.apache.polaris.core.auth.PolarisAuthorizableOperation.REFERENCE_LINEAGE_INPUT_TABLE;
import static org.apache.polaris.service.lineage.LineageTestEvents.datasetFreeEvent;
import static org.apache.polaris.service.lineage.LineageTestEvents.input;
import static org.apache.polaris.service.lineage.LineageTestEvents.output;
import static org.apache.polaris.service.lineage.LineageTestEvents.runEvent;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.server.OpenLineage;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.auth.AuthorizationDecision;
import org.apache.polaris.core.auth.AuthorizationRequest;
import org.apache.polaris.core.auth.PathSegment;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.PolarisSecurable;
import org.apache.polaris.core.auth.SingleTargetAuthorizationIntent;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.service.catalog.common.PolarisSecurableMapper;
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

  /** A Polaris Iceberg REST catalog endpoint, so datasets under it are Polaris-native. */
  private static final String NS = "http://polaris:8181/api/catalog";

  private OpenLineageIngestProvider provider;
  private RealmConfig realmConfig;
  private OpenLineageAdapter adapter;

  private final Map<String, Set<ResolvedPathKey>> resolvable = new HashMap<>();
  private final Set<String> grants = new HashSet<>();

  @BeforeEach
  void setUp() {
    provider = mock(OpenLineageIngestProvider.class);
    realmConfig = mock(RealmConfig.class);
    when(realmConfig.getConfig(FeatureConfiguration.ENABLE_OPENLINEAGE_INGEST)).thenReturn(true);

    PolarisPrincipal principal = mock(PolarisPrincipal.class);
    when(principal.getName()).thenReturn("alice");

    ResolutionManifestFactory manifestFactory = mock(ResolutionManifestFactory.class);
    when(manifestFactory.createResolutionManifest(any(), nullable(String.class)))
        .thenAnswer(invocation -> fakeManifest(invocation.getArgument(1)));

    PolarisAuthorizer authorizer = mock(PolarisAuthorizer.class);
    when(authorizer.authorize(any(), any()))
        .thenAnswer(
            invocation -> {
              AuthorizationRequest request = invocation.getArgument(1);
              SingleTargetAuthorizationIntent intent =
                  (SingleTargetAuthorizationIntent) request.intents().get(0);
              return grants.contains(grantKey(intent.operation(), intent.target()))
                  ? AuthorizationDecision.allow()
                  : AuthorizationDecision.deny("denied");
            });

    adapter =
        new OpenLineageAdapter(
            provider,
            realmConfig,
            new LineageIngestAuthorizer(
                principal, manifestFactory, authorizer, new PolarisDatasetIdentifier(Map.of())));
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
      tableExists("cat", "ns", "secret");
      tableExists("cat", "ns", "dst");
      grant(INGEST_LINEAGE, table("cat", "ns", "dst"));

      Response response =
          adapter.sendLineageEvent(
              event(
                  runEvent(
                      List.of(input(NS, "cat.ns.secret")), List.of(output(NS, "cat.ns.dst")))));

      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());

      OpenLineage.RunEvent forwarded = (OpenLineage.RunEvent) capturedEvent();
      assertThat(forwarded.getInputs()).isEmpty();
      assertThat(forwarded.getOutputs())
          .extracting(OpenLineage.Dataset::getName)
          .containsExactly("cat.ns.dst");
      // Nothing in the forwarded event mentions the unreadable table at all, in either direction.
      assertThat(allDatasetNames(forwarded)).doesNotContain("cat.ns.secret");

      assertThat(response.getHeaderString(OpenLineageAdapter.OMITTED_UNAUTHORIZED_HEADER))
          .isEqualTo("1");
      assertThat(response.getHeaderString(OpenLineageAdapter.OMITTED_UNRESOLVED_HEADER)).isNull();
    }

    @Test
    void anInputWithOnlyReadAccessIsForwarded() {
      // The ordinary ETL case that motivated splitting REFERENCE_LINEAGE_INPUT_TABLE out.
      when(provider.ingest(any())).thenReturn(OpenLineageIngestResult.ACCEPTED);
      tableExists("cat", "ns", "src");
      grant(REFERENCE_LINEAGE_INPUT_TABLE, table("cat", "ns", "src"));

      Response response =
          adapter.sendLineageEvent(event(runEvent(List.of(input(NS, "cat.ns.src")), List.of())));

      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
      assertThat(allDatasetNames((OpenLineage.RunEvent) capturedEvent()))
          .containsExactly("cat.ns.src");
    }

    @Test
    void aCreateTableAsSelectOutputIsAuthorizedAgainstItsParentNamespace() {
      when(provider.ingest(any())).thenReturn(OpenLineageIngestResult.ACCEPTED);
      namespaceExists("cat", "ns");
      grant(INGEST_LINEAGE, namespace("cat", "ns"));

      Response response =
          adapter.sendLineageEvent(
              event(runEvent(List.of(), List.of(output(NS, "cat.ns.brand_new")))));

      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
      assertThat(allDatasetNames((OpenLineage.RunEvent) capturedEvent()))
          .containsExactly("cat.ns.brand_new");
    }

    @Test
    void externalDatasetsAreRetainedBesideAnAuthorizedPolarisEntity() {
      when(provider.ingest(any())).thenReturn(OpenLineageIngestResult.ACCEPTED);
      tableExists("cat", "ns", "dst");
      grant(INGEST_LINEAGE, table("cat", "ns", "dst"));

      adapter.sendLineageEvent(
          event(
              runEvent(
                  List.of(input("kafka://broker", "topic")), List.of(output(NS, "cat.ns.dst")))));

      assertThat(allDatasetNames((OpenLineage.RunEvent) capturedEvent()))
          .containsExactlyInAnyOrder("topic", "cat.ns.dst");
    }

    @Test
    void anEventThatAuthorizesNothingSucceedsAsANoOpWithoutCallingTheProvider() {
      tableExists("cat", "ns", "a");
      tableExists("cat", "ns", "b");
      // No grants at all: everything is denied.

      Response response =
          adapter.sendLineageEvent(
              event(
                  runEvent(
                      List.of(input(NS, "cat.ns.a")),
                      List.of(output(NS, "cat.ns.b"), output(NS, "cat.ns.missing")))));

      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
      verify(provider, never()).ingest(any());
      assertThat(response.getHeaderString(OpenLineageAdapter.OMITTED_UNAUTHORIZED_HEADER))
          .isEqualTo("2");
      assertThat(response.getHeaderString(OpenLineageAdapter.OMITTED_UNRESOLVED_HEADER))
          .isEqualTo("1");
    }

    @Test
    void anAllExternalEventSucceedsAsANoOp() {
      Response response =
          adapter.sendLineageEvent(
              event(runEvent(List.of(input("kafka://broker", "topic")), List.of())));

      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
      verify(provider, never()).ingest(any());
      // Nothing was omitted for an authorization reason, so no advisory header is emitted.
      assertThat(response.getHeaderString(OpenLineageAdapter.OMITTED_UNAUTHORIZED_HEADER)).isNull();
      assertThat(response.getHeaderString(OpenLineageAdapter.OMITTED_UNRESOLVED_HEADER)).isNull();
    }

    @Test
    void anEventOfUnenumerableShapeSucceedsAsANoOp() {
      Response response = adapter.sendLineageEvent(new PolarisLineageEvent(null));

      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
      verify(provider, never()).ingest(any());
    }

    @Test
    void noOmissionHeadersAreEmittedWhenNothingWasOmitted() {
      when(provider.ingest(any())).thenReturn(OpenLineageIngestResult.ACCEPTED);

      Response response = adapter.sendLineageEvent(event(datasetFreeEvent()));

      assertThat(response.getHeaderString(OpenLineageAdapter.OMITTED_UNRESOLVED_HEADER)).isNull();
      assertThat(response.getHeaderString(OpenLineageAdapter.OMITTED_UNAUTHORIZED_HEADER)).isNull();
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
      assertThat(body.summary().omittedUnresolved()).isZero();
      assertThat(body.summary().omittedUnauthorized()).isZero();
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
      tableExists("cat", "ns", "denied");

      Response response =
          adapter.sendLineageEventBatch(
              List.of(
                  event(datasetFreeEvent()),
                  // Authorizes nothing: one existing-but-denied input, one nonexistent input.
                  event(
                      runEvent(
                          List.of(input(NS, "cat.ns.denied"), input(NS, "cat.ns.missing")),
                          List.of()))));

      OpenLineageBatchIngestResponse body = body(response);
      assertThat(body.status()).isEqualTo(OpenLineageBatchIngestResponse.Status.SUCCESS);
      assertThat(body.summary().received()).isEqualTo(2);
      assertThat(body.summary().successful()).isEqualTo(2);
      assertThat(body.summary().failed()).isZero();
      assertThat(body.summary().omittedUnauthorized()).isEqualTo(1);
      assertThat(body.summary().omittedUnresolved()).isEqualTo(1);
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
    void omittedCountsAccumulateAcrossTheBatch() {
      when(provider.ingest(any())).thenReturn(OpenLineageIngestResult.ACCEPTED);
      tableExists("cat", "ns", "dst");
      tableExists("cat", "ns", "denied");
      grant(INGEST_LINEAGE, table("cat", "ns", "dst"));

      PolarisLineageEvent partiallyAuthorized =
          event(
              runEvent(
                  List.of(input(NS, "cat.ns.denied"), input(NS, "cat.ns.missing")),
                  List.of(output(NS, "cat.ns.dst"))));

      Response response =
          adapter.sendLineageEventBatch(List.of(partiallyAuthorized, partiallyAuthorized));

      OpenLineageBatchIngestResponse body = body(response);
      assertThat(body.status()).isEqualTo(OpenLineageBatchIngestResponse.Status.SUCCESS);
      assertThat(body.summary().successful()).isEqualTo(2);
      assertThat(body.summary().omittedUnauthorized()).isEqualTo(2);
      assertThat(body.summary().omittedUnresolved()).isEqualTo(2);
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

  private PolarisResolutionManifest fakeManifest(String catalog) {
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    List<ResolverPath> paths = new ArrayList<>();
    doAnswer(
            invocation -> {
              paths.add(invocation.getArgument(0));
              return null;
            })
        .when(manifest)
        .addPath(any());
    when(manifest.getResolvedPath(any(ResolvedPathKey.class), anyBoolean()))
        .thenAnswer(
            invocation -> {
              ResolvedPathKey key = invocation.getArgument(0);
              assertThat(paths).extracting(ResolverPath::key).contains(key);
              return resolvable.getOrDefault(catalog, Set.of()).contains(key)
                  ? new PolarisResolvedPathWrapper(List.of())
                  : null;
            });
    return manifest;
  }

  private void tableExists(String catalog, String... namespaceAndTable) {
    resolvable
        .computeIfAbsent(catalog, c -> new HashSet<>())
        .add(ResolvedPathKey.of(List.of(namespaceAndTable), PolarisEntityType.TABLE_LIKE));
  }

  private void namespaceExists(String catalog, String... levels) {
    resolvable
        .computeIfAbsent(catalog, c -> new HashSet<>())
        .add(ResolvedPathKey.of(List.of(levels), PolarisEntityType.NAMESPACE));
  }

  private void grant(PolarisAuthorizableOperation operation, PolarisSecurable securable) {
    grants.add(grantKey(operation, securable));
  }

  private static PolarisSecurable table(String catalog, String namespace, String table) {
    return PolarisSecurableMapper.tableLike(
        catalog, TableIdentifier.of(Namespace.of(namespace), table));
  }

  private static PolarisSecurable namespace(String catalog, String namespace) {
    return PolarisSecurableMapper.namespace(catalog, Namespace.of(namespace));
  }

  private static String grantKey(
      PolarisAuthorizableOperation operation, PolarisSecurable securable) {
    return operation
        + ":"
        + securable.getLeaf().entityType()
        + ":"
        + securable.getPathSegments().stream()
            .map(PathSegment::name)
            .collect(Collectors.joining(" "));
  }
}
