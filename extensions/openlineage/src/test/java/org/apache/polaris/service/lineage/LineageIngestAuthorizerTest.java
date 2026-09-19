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
import static org.apache.polaris.service.lineage.LineageTestEvents.datasetEvent;
import static org.apache.polaris.service.lineage.LineageTestEvents.input;
import static org.apache.polaris.service.lineage.LineageTestEvents.output;
import static org.apache.polaris.service.lineage.LineageTestEvents.runEvent;
import static org.apache.polaris.service.lineage.LineageTestEvents.staticDataset;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
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
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.service.catalog.common.PolarisSecurableMapper;
import org.apache.polaris.service.lineage.LineageAuthorizationResult.Disposition;
import org.apache.polaris.service.lineage.LineageDatasetKey.Role;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link LineageIngestAuthorizer}.
 *
 * <p>The fixture stands in for a catalog by holding a set of resolvable path keys and a set of
 * granted {@code (operation, securable)} pairs, so a test can say "this table exists and the caller
 * may read it" and then assert the resulting disposition. The fake manifest also enforces the real
 * manifest's contract that a lookup key must have been registered before it is queried, so a
 * missing {@code addPath} fails the test rather than silently reading as unresolved.
 */
class LineageIngestAuthorizerTest {

  /**
   * A Polaris Iceberg REST catalog endpoint, so these datasets are Polaris-native without any
   * configured namespace mapping. The namespace is what makes them Polaris, not the dotted name.
   */
  private static final String DATASET_NAMESPACE = "http://polaris:8181/api/catalog";

  private PolarisPrincipal principal;
  private PolarisAuthorizer authorizer;
  private LineageIngestAuthorizer ingestAuthorizer;

  /** Path keys that resolve, per catalog. Anything absent does not exist for this caller. */
  private final Map<String, Set<ResolvedPathKey>> resolvable = new HashMap<>();

  /** Granted (operation, securable) pairs. */
  private final Set<String> grants = new HashSet<>();

  private final Map<String, List<ResolverPath>> registeredPaths = new LinkedHashMap<>();
  private final List<String> manifestsCreatedFor = new ArrayList<>();
  private final List<AuthorizationRequest> resolveRequests = new ArrayList<>();
  private final List<AuthorizationRequest> authorizeRequests = new ArrayList<>();
  private final List<Boolean> prependRootContainerFlags = new ArrayList<>();

  @BeforeEach
  void setUp() {
    principal = mock(PolarisPrincipal.class);
    when(principal.getName()).thenReturn("alice");

    ResolutionManifestFactory manifestFactory = mock(ResolutionManifestFactory.class);
    when(manifestFactory.createResolutionManifest(any(), nullable(String.class)))
        .thenAnswer(
            invocation -> {
              String catalog = invocation.getArgument(1);
              manifestsCreatedFor.add(catalog);
              return fakeManifest(catalog);
            });

    authorizer = mock(PolarisAuthorizer.class);
    doAnswer(
            invocation -> {
              resolveRequests.add(invocation.getArgument(1));
              return null;
            })
        .when(authorizer)
        .resolveAuthorizationInputs(any(), any());
    when(authorizer.authorize(any(), any()))
        .thenAnswer(
            invocation -> {
              AuthorizationRequest request = invocation.getArgument(1);
              authorizeRequests.add(request);
              // Each decision must be its own request: a multi-intent request is AND-combined into
              // one bit, which cannot classify datasets independently.
              assertThat(request.intents()).hasSize(1);
              SingleTargetAuthorizationIntent intent =
                  (SingleTargetAuthorizationIntent) request.intents().get(0);
              return grants.contains(grantKey(intent.operation(), intent.target()))
                  ? AuthorizationDecision.allow()
                  : AuthorizationDecision.deny("denied");
            });

    ingestAuthorizer =
        new LineageIngestAuthorizer(
            principal, manifestFactory, authorizer, new PolarisDatasetIdentifier(Map.of()));
  }

  // ---------------------------------------------------------------------------------------------
  // The cases that motivated the design.
  // ---------------------------------------------------------------------------------------------

  @Test
  void inputWithOnlyReadAccessIsAuthorized() {
    // The ordinary ETL case. Ingest rides write-only uber-grants, so requiring LINEAGE_INGEST on
    // inputs would make a job that merely reads a source table record nothing at all.
    tableExists("cat", "ns", "src");
    grant(REFERENCE_LINEAGE_INPUT_TABLE, table("cat", "ns", "src"));

    LineageAuthorizationResult result =
        ingestAuthorizer.authorize(
            runEvent(List.of(input(DATASET_NAMESPACE, "cat.ns.src")), List.of()));

    assertThat(result.dispositions())
        .containsExactly(
            Map.entry(
                new LineageDatasetKey(Role.INPUT, DATASET_NAMESPACE, "cat.ns.src"),
                Disposition.AUTHORIZED));
  }

  @Test
  void anUnreadableInputIsDroppedWhileAWritableOutputIsAuthorized() {
    // THE FORGERY VECTOR. A caller who can write cat.ns.dst must not be able to assert that dst was
    // derived from cat.ns.secret, a table they cannot even read. The output stands; the input does
    // not, so no edge involving secret can be persisted.
    tableExists("cat", "ns", "secret");
    tableExists("cat", "ns", "dst");
    grant(INGEST_LINEAGE, table("cat", "ns", "dst"));
    // Deliberately no REFERENCE_LINEAGE_INPUT_TABLE on secret.

    LineageAuthorizationResult result =
        ingestAuthorizer.authorize(
            runEvent(
                List.of(input(DATASET_NAMESPACE, "cat.ns.secret")),
                List.of(output(DATASET_NAMESPACE, "cat.ns.dst"))));

    assertThat(result.dispositions())
        .containsExactly(
            Map.entry(
                new LineageDatasetKey(Role.INPUT, DATASET_NAMESPACE, "cat.ns.secret"),
                Disposition.DROPPED_UNAUTHORIZED),
            Map.entry(
                new LineageDatasetKey(Role.OUTPUT, DATASET_NAMESPACE, "cat.ns.dst"),
                Disposition.AUTHORIZED));
    assertThat(result.isNoOp()).isFalse();
    assertThat(result.requiresFiltering()).isTrue();
    assertThat(result.omittedUnauthorizedCount()).isEqualTo(1);
    assertThat(result.omittedUnresolvedCount()).isZero();
  }

  @Test
  void anOutputTableThatDoesNotExistYetFallsBackToItsParentNamespace() {
    // Create-table-as-select: the event legitimately names a table that does not exist yet.
    namespaceExists("cat", "ns");
    grant(INGEST_LINEAGE, namespace("cat", "ns"));

    LineageAuthorizationResult result =
        ingestAuthorizer.authorize(
            runEvent(List.of(), List.of(output(DATASET_NAMESPACE, "cat.ns.brand_new"))));

    assertThat(result.dispositions().values()).containsExactly(Disposition.AUTHORIZED);
    assertThat(authorizeRequests).hasSize(1);
    assertThat(operationOf(authorizeRequests.get(0))).isEqualTo(INGEST_LINEAGE);
  }

  @Test
  void anOutputTableThatExistsUsesTheTableGrantAndNotTheNamespaceFallback() {
    tableExists("cat", "ns", "dst");
    namespaceExists("cat", "ns");
    grant(INGEST_LINEAGE, table("cat", "ns", "dst"));
    grant(INGEST_LINEAGE, namespace("cat", "ns"));

    LineageAuthorizationResult result =
        ingestAuthorizer.authorize(
            runEvent(List.of(), List.of(output(DATASET_NAMESPACE, "cat.ns.dst"))));

    assertThat(result.dispositions().values()).containsExactly(Disposition.AUTHORIZED);
    assertThat(authorizeRequests).hasSize(1);
    assertThat(operationOf(authorizeRequests.get(0))).isEqualTo(INGEST_LINEAGE);
  }

  @Test
  void anOutputWhoseTableAndNamespaceBothFailToResolveIsDroppedUnresolved() {
    LineageAuthorizationResult result =
        ingestAuthorizer.authorize(
            runEvent(List.of(), List.of(output(DATASET_NAMESPACE, "cat.ns.dst"))));

    assertThat(result.dispositions().values()).containsExactly(Disposition.DROPPED_UNRESOLVED);
    assertThat(authorizeRequests).isEmpty();
  }

  @Test
  void anInputThatDoesNotResolveNeverFallsBackToItsNamespace() {
    // Allowing the fallback would turn a namespace-level grant into "you may assert provenance from
    // any name under this namespace", which is exactly the claim the per-dataset check prevents.
    namespaceExists("cat", "ns");
    grant(INGEST_LINEAGE, namespace("cat", "ns"));
    grant(REFERENCE_LINEAGE_INPUT_TABLE, table("cat", "ns", "ghost"));

    LineageAuthorizationResult result =
        ingestAuthorizer.authorize(
            runEvent(List.of(input(DATASET_NAMESPACE, "cat.ns.ghost")), List.of()));

    assertThat(result.dispositions().values()).containsExactly(Disposition.DROPPED_UNRESOLVED);
    assertThat(authorizeRequests).isEmpty();
    // The namespace path is not even registered for an input, so no namespace grant can be
    // consulted.
    assertThat(registeredPathKeys("cat")).doesNotContain(namespaceKey("ns"));
  }

  @Test
  void aStandaloneDatasetThatDoesNotResolveNeverFallsBackToItsNamespace() {
    namespaceExists("cat", "ns");
    grant(INGEST_LINEAGE, namespace("cat", "ns"));

    LineageAuthorizationResult result =
        ingestAuthorizer.authorize(datasetEvent(staticDataset(DATASET_NAMESPACE, "cat.ns.ghost")));

    assertThat(result.dispositions().values()).containsExactly(Disposition.DROPPED_UNRESOLVED);
    assertThat(authorizeRequests).isEmpty();
    assertThat(registeredPathKeys("cat")).doesNotContain(namespaceKey("ns"));
  }

  @Test
  void aStandaloneDatasetThatResolvesNeedsIngestOnTheTable() {
    tableExists("cat", "ns", "tbl");
    grant(INGEST_LINEAGE, table("cat", "ns", "tbl"));

    LineageAuthorizationResult result =
        ingestAuthorizer.authorize(datasetEvent(staticDataset(DATASET_NAMESPACE, "cat.ns.tbl")));

    assertThat(result.dispositions().values()).containsExactly(Disposition.AUTHORIZED);
    assertThat(operationOf(authorizeRequests.get(0))).isEqualTo(INGEST_LINEAGE);
  }

  @Test
  void oneTableAppearingAsBothInputAndOutputGetsTwoIndependentDecisions() {
    // MERGE INTO. The caller may record that they wrote the table without being able to assert they
    // read it, so the two occurrences resolve differently.
    tableExists("cat", "ns", "tbl");
    grant(INGEST_LINEAGE, table("cat", "ns", "tbl"));

    LineageAuthorizationResult result =
        ingestAuthorizer.authorize(
            runEvent(
                List.of(input(DATASET_NAMESPACE, "cat.ns.tbl")),
                List.of(output(DATASET_NAMESPACE, "cat.ns.tbl"))));

    assertThat(result.dispositions())
        .containsExactly(
            Map.entry(
                new LineageDatasetKey(Role.INPUT, DATASET_NAMESPACE, "cat.ns.tbl"),
                Disposition.DROPPED_UNAUTHORIZED),
            Map.entry(
                new LineageDatasetKey(Role.OUTPUT, DATASET_NAMESPACE, "cat.ns.tbl"),
                Disposition.AUTHORIZED));
  }

  // ---------------------------------------------------------------------------------------------
  // External and unaddressable datasets.
  // ---------------------------------------------------------------------------------------------

  @Test
  void externalDatasetsAreRetainedWithoutAnyAuthorizationCall() {
    tableExists("cat", "ns", "dst");
    grant(INGEST_LINEAGE, table("cat", "ns", "dst"));

    LineageAuthorizationResult result =
        ingestAuthorizer.authorize(
            runEvent(
                List.of(input("kafka://broker", "topic"), input("postgres://db", "public.users")),
                List.of(output(DATASET_NAMESPACE, "cat.ns.dst"))));

    assertThat(result.dispositions())
        .containsExactly(
            Map.entry(
                new LineageDatasetKey(Role.INPUT, "kafka://broker", "topic"), Disposition.EXTERNAL),
            Map.entry(
                new LineageDatasetKey(Role.INPUT, "postgres://db", "public.users"),
                Disposition.EXTERNAL),
            Map.entry(
                new LineageDatasetKey(Role.OUTPUT, DATASET_NAMESPACE, "cat.ns.dst"),
                Disposition.AUTHORIZED));
    assertThat(result.externalCount()).isEqualTo(2);
    assertThat(result.requiresFiltering()).isFalse();
    // External datasets have no securable, so only the Polaris output produced a decision.
    assertThat(authorizeRequests).hasSize(1);
    assertThat(manifestsCreatedFor).containsExactly("cat");
  }

  @Test
  void anEventOfOnlyExternalDatasetsIsANoOp() {
    LineageAuthorizationResult result =
        ingestAuthorizer.authorize(
            runEvent(
                List.of(input("kafka://broker", "topic")), List.of(output("s3://raw", "blob"))));

    assertThat(result.dispositions().values())
        .containsExactly(Disposition.EXTERNAL, Disposition.EXTERNAL);
    assertThat(result.isNoOp()).isTrue();
    assertThat(manifestsCreatedFor).isEmpty();
  }

  @Test
  void anUnaddressablePolarisClaimIsDroppedAsUnresolvedAndNeverAuthorized() {
    // "cat..dst" claims a Polaris catalog but is not an addressable path. It must not be recorded
    // as
    // an external node beside the real cat.dst.
    tableExists("cat", "ns", "dst");
    grant(INGEST_LINEAGE, table("cat", "ns", "dst"));

    LineageAuthorizationResult result =
        ingestAuthorizer.authorize(
            runEvent(
                List.of(input(DATASET_NAMESPACE, "cat..dst")),
                List.of(output(DATASET_NAMESPACE, "cat.ns.dst"))));

    assertThat(result.dispositions())
        .containsEntry(
            new LineageDatasetKey(Role.INPUT, DATASET_NAMESPACE, "cat..dst"),
            Disposition.DROPPED_UNRESOLVED);
    assertThat(result.omittedUnresolvedCount()).isEqualTo(1);
    // Only the real output contributed paths (its table plus its namespace fallback): the malformed
    // claim was rejected by the identity rules without spending a resolution or a decision.
    assertThat(registeredPathKeys("cat"))
        .containsExactlyInAnyOrder(tableKey("ns", "dst"), namespaceKey("ns"));
    assertThat(authorizeRequests).hasSize(1);
  }

  // ---------------------------------------------------------------------------------------------
  // Resolution strategy invariants.
  // ---------------------------------------------------------------------------------------------

  @Test
  void everyRegisteredPathIsOptional() {
    // Load-bearing: one non-optional miss fails resolveAll, after which every lookup returns null
    // and the authorization intent trips a precondition, turning a drop into a 500.
    tableExists("cat", "ns", "src");

    ingestAuthorizer.authorize(
        runEvent(
            List.of(
                input(DATASET_NAMESPACE, "cat.ns.src"), input(DATASET_NAMESPACE, "cat.ns.gone")),
            List.of(output(DATASET_NAMESPACE, "cat.ns.dst"))));

    assertThat(registeredPaths.get("cat")).isNotEmpty().allMatch(ResolverPath::optional);
  }

  @Test
  void theNamespacePathIsRegisteredForOutputsOnly() {
    ingestAuthorizer.authorize(
        runEvent(
            List.of(input(DATASET_NAMESPACE, "cat.in_ns.src")),
            List.of(output(DATASET_NAMESPACE, "cat.out_ns.dst"))));

    assertThat(registeredPathKeys("cat"))
        .contains(namespaceKey("out_ns"))
        .doesNotContain(namespaceKey("in_ns"));
  }

  @Test
  void pathsAreReadBackWithTheSameRootingTheAuthorizerUses() {
    // All four lineage operations register with the default ResolvedPathRooting.ROOT, so
    // PolarisAuthorizerImpl reads their paths back with prependRootContainer = true. The probe
    // reads
    // the same view, matching every read-back in CatalogHandler.
    namespaceExists("cat", "ns");

    ingestAuthorizer.authorize(
        runEvent(
            List.of(input(DATASET_NAMESPACE, "cat.ns.src")),
            List.of(output(DATASET_NAMESPACE, "cat.ns.dst"))));

    assertThat(prependRootContainerFlags).isNotEmpty().containsOnly(true);
  }

  @Test
  void resolutionHappensExactlyOncePerCatalog() {
    // A manifest resolves once and only once, and the point of the shared manifest is that N
    // datasets cost one resolution round rather than N.
    ingestAuthorizer.authorize(
        runEvent(
            List.of(
                input(DATASET_NAMESPACE, "cat.ns.a"),
                input(DATASET_NAMESPACE, "cat.ns.b"),
                input(DATASET_NAMESPACE, "cat.ns.c")),
            List.of(output(DATASET_NAMESPACE, "cat.ns.d"))));

    assertThat(manifestsCreatedFor).containsExactly("cat");
    assertThat(resolveRequests).hasSize(1);
  }

  @Test
  void eachDistinctCatalogGetsItsOwnManifest() {
    // A manifest binds one reference catalog at construction, so datasets must be grouped first.
    tableExists("cat_a", "ns", "a");
    tableExists("cat_b", "ns", "b");
    grant(REFERENCE_LINEAGE_INPUT_TABLE, table("cat_a", "ns", "a"));
    grant(REFERENCE_LINEAGE_INPUT_TABLE, table("cat_b", "ns", "b"));

    LineageAuthorizationResult result =
        ingestAuthorizer.authorize(
            runEvent(
                List.of(
                    input(DATASET_NAMESPACE, "cat_a.ns.a"), input(DATASET_NAMESPACE, "cat_b.ns.b")),
                List.of()));

    assertThat(manifestsCreatedFor).containsExactlyInAnyOrder("cat_a", "cat_b");
    assertThat(resolveRequests).hasSize(2);
    assertThat(result.authorizedCount()).isEqualTo(2);
  }

  @Test
  void repeatedPathKeysAreRegisteredOnlyOnce() {
    // Two outputs in the same namespace share one namespace path.
    ingestAuthorizer.authorize(
        runEvent(
            List.of(),
            List.of(output(DATASET_NAMESPACE, "cat.ns.a"), output(DATASET_NAMESPACE, "cat.ns.b"))));

    assertThat(registeredPathKeys("cat"))
        .containsExactlyInAnyOrder(tableKey("ns", "a"), tableKey("ns", "b"), namespaceKey("ns"));
  }

  @Test
  void theResolutionHintCarriesEveryCandidateIntentForTheCatalog() {
    // resolveAuthorizationInputs is the one hook that triggers resolveAll, so a pluggable
    // authorizer
    // must see every securable the pass may later ask about — including the output namespace
    // fallback. It is a hint only; decisions are taken from single-intent requests.
    ingestAuthorizer.authorize(
        runEvent(
            List.of(input(DATASET_NAMESPACE, "cat.ns.src")),
            List.of(output(DATASET_NAMESPACE, "cat.ns.dst"))));

    assertThat(resolveRequests).hasSize(1);
    assertThat(resolveRequests.get(0).intents())
        .hasSize(3)
        .extracting(intent -> ((SingleTargetAuthorizationIntent) intent).operation())
        .containsExactlyInAnyOrder(REFERENCE_LINEAGE_INPUT_TABLE, INGEST_LINEAGE, INGEST_LINEAGE);
  }

  @Test
  void aCatalogThatDoesNotExistDropsEveryDatasetInItWithoutThrowing() {
    // A missing reference catalog fails resolveAll, which the optional-path design turns into
    // "every
    // path in this catalog is unresolved" rather than a request failure.
    var event =
        runEvent(
            List.of(input(DATASET_NAMESPACE, "nope.ns.a")),
            List.of(output(DATASET_NAMESPACE, "nope.ns.b")));

    assertThatCode(() -> ingestAuthorizer.authorize(event)).doesNotThrowAnyException();

    LineageAuthorizationResult result = ingestAuthorizer.authorize(event);

    assertThat(result.dispositions().values()).containsOnly(Disposition.DROPPED_UNRESOLVED);
    assertThat(result.isNoOp()).isTrue();
    assertThat(authorizeRequests).isEmpty();
  }

  // ---------------------------------------------------------------------------------------------
  // Aggregate reporting and event-level outcomes.
  // ---------------------------------------------------------------------------------------------

  @Test
  void unresolvedAndUnauthorizedAreCountedSeparately() {
    tableExists("cat", "ns", "denied_a");
    tableExists("cat", "ns", "denied_b");
    tableExists("cat", "ns", "ok");
    grant(REFERENCE_LINEAGE_INPUT_TABLE, table("cat", "ns", "ok"));

    LineageAuthorizationResult result =
        ingestAuthorizer.authorize(
            runEvent(
                List.of(
                    input(DATASET_NAMESPACE, "cat.ns.ok"),
                    input(DATASET_NAMESPACE, "cat.ns.denied_a"),
                    input(DATASET_NAMESPACE, "cat.ns.denied_b"),
                    input(DATASET_NAMESPACE, "cat.ns.missing")),
                List.of()));

    assertThat(result.authorizedCount()).isEqualTo(1);
    assertThat(result.omittedUnauthorizedCount()).isEqualTo(2);
    assertThat(result.omittedUnresolvedCount()).isEqualTo(1);
    assertThat(result.hasOmissions()).isTrue();
  }

  @Test
  void anEventWhereEverythingIsDeniedIsANoOp() {
    tableExists("cat", "ns", "a");

    LineageAuthorizationResult result =
        ingestAuthorizer.authorize(
            runEvent(List.of(input(DATASET_NAMESPACE, "cat.ns.a")), List.of()));

    assertThat(result.isNoOp()).isTrue();
    assertThat(result.authorizedCount()).isZero();
    assertThat(result.omittedUnauthorizedCount()).isEqualTo(1);
  }

  @Test
  void anEventThatReferencesNoDatasetsIsNotANoOp() {
    // Job-lifecycle traffic. It makes no claim about any Polaris entity, so there is nothing to
    // authorize and nothing to forge; discarding it would lose real signal for no security gain.
    LineageAuthorizationResult result =
        ingestAuthorizer.authorize(LineageTestEvents.datasetFreeEvent());

    assertThat(result.dispositions()).isEmpty();
    assertThat(result.isNoOp()).isFalse();
    assertThat(result.requiresFiltering()).isFalse();
    assertThat(result.hasOmissions()).isFalse();
    assertThat(manifestsCreatedFor).isEmpty();
  }

  @Test
  void anEventOfUnenumerableShapeIsANoOp() {
    // Its dataset references cannot be seen, so it cannot be authorized and must not be forwarded.
    assertThat(ingestAuthorizer.authorize(null).isNoOp()).isTrue();
    assertThat(manifestsCreatedFor).isEmpty();
  }

  @Test
  void aConfiguredNamespaceMappingRoutesDatasetsToTheMappedCatalog() {
    LineageIngestAuthorizer configured =
        new LineageIngestAuthorizer(
            principal,
            (subject, catalog) -> fakeManifestFor(catalog),
            authorizer,
            new PolarisDatasetIdentifier(Map.of(DATASET_NAMESPACE, "mapped_cat")));

    tableExists("mapped_cat", "ns", "tbl");
    grant(REFERENCE_LINEAGE_INPUT_TABLE, table("mapped_cat", "ns", "tbl"));

    LineageAuthorizationResult result =
        configured.authorize(runEvent(List.of(input(DATASET_NAMESPACE, "ns.tbl")), List.of()));

    assertThat(result.dispositions().values()).containsExactly(Disposition.AUTHORIZED);
    assertThat(manifestsCreatedFor).containsExactly("mapped_cat");
  }

  // ---------------------------------------------------------------------------------------------
  // Fixture.
  // ---------------------------------------------------------------------------------------------

  private PolarisResolutionManifest fakeManifestFor(String catalog) {
    manifestsCreatedFor.add(catalog);
    return fakeManifest(catalog);
  }

  private PolarisResolutionManifest fakeManifest(String catalog) {
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    List<ResolverPath> paths = registeredPaths.computeIfAbsent(catalog, c -> new ArrayList<>());

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
              prependRootContainerFlags.add(invocation.getArgument(1));
              // The real manifest fails a diagnostic check when a key was never registered, so a
              // missing addPath must fail the test rather than read as "unresolved".
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

  private List<ResolvedPathKey> registeredPathKeys(String catalog) {
    return registeredPaths.getOrDefault(catalog, List.of()).stream()
        .map(ResolverPath::key)
        .toList();
  }

  private static ResolvedPathKey tableKey(String namespace, String table) {
    return ResolvedPathKey.of(List.of(namespace, table), PolarisEntityType.TABLE_LIKE);
  }

  private static ResolvedPathKey namespaceKey(String... levels) {
    return ResolvedPathKey.of(List.of(levels), PolarisEntityType.NAMESPACE);
  }

  private static PolarisAuthorizableOperation operationOf(AuthorizationRequest request) {
    return ((SingleTargetAuthorizationIntent) request.intents().get(0)).operation();
  }
}
