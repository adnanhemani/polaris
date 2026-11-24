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
import static org.apache.polaris.core.auth.PolarisAuthorizableOperation.INGEST_VIEW_LINEAGE;
import static org.apache.polaris.core.auth.PolarisAuthorizableOperation.REFERENCE_LINEAGE_INPUT_TABLE;
import static org.apache.polaris.core.auth.PolarisAuthorizableOperation.REFERENCE_LINEAGE_INPUT_VIEW;
import static org.apache.polaris.service.lineage.FakeLineageCatalog.CATALOG_URI;
import static org.apache.polaris.service.lineage.FakeLineageCatalog.namespace;
import static org.apache.polaris.service.lineage.FakeLineageCatalog.namespaceKey;
import static org.apache.polaris.service.lineage.FakeLineageCatalog.operationOf;
import static org.apache.polaris.service.lineage.FakeLineageCatalog.sparkInput;
import static org.apache.polaris.service.lineage.FakeLineageCatalog.sparkOutput;
import static org.apache.polaris.service.lineage.FakeLineageCatalog.table;
import static org.apache.polaris.service.lineage.FakeLineageCatalog.tableKey;
import static org.apache.polaris.service.lineage.LineageTestEvents.datasetEvent;
import static org.apache.polaris.service.lineage.LineageTestEvents.input;
import static org.apache.polaris.service.lineage.LineageTestEvents.inputWithFacets;
import static org.apache.polaris.service.lineage.LineageTestEvents.output;
import static org.apache.polaris.service.lineage.LineageTestEvents.runEvent;
import static org.apache.polaris.service.lineage.LineageTestEvents.sparkFacets;
import static org.apache.polaris.service.lineage.LineageTestEvents.staticDatasetWithFacets;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.List;
import java.util.stream.Stream;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.SingleTargetAuthorizationIntent;
import org.apache.polaris.service.lineage.LineageAuthorizationResult.Disposition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link LineageIngestAuthorizer}, driven through {@link FakeLineageCatalog}.
 *
 * <p>Datasets are built the way OpenLineage Spark names a table in an Iceberg REST catalog: by
 * storage location, with the catalog and the table's name in the facets.
 */
class LineageIngestAuthorizerTest {

  private FakeLineageCatalog catalog;
  private LineageIngestAuthorizer ingestAuthorizer;

  @BeforeEach
  void setUp() {
    catalog = new FakeLineageCatalog();
    ingestAuthorizer = catalog.ingestAuthorizer();
  }

  // ---------------------------------------------------------------------------------------------
  // The cases that motivated the design.
  // ---------------------------------------------------------------------------------------------

  @Test
  void inputWithOnlyReadAccessIsAuthorized() {
    // The ordinary ETL case. Ingest rides write-only grants, so requiring it on inputs would make a
    // job that merely reads a source table record nothing at all.
    catalog.tableExists("cat", "ns", "src");
    catalog.grant(REFERENCE_LINEAGE_INPUT_TABLE, table("cat", "ns", "src"));

    LineageAuthorizationResult result =
        ingestAuthorizer.authorize(runEvent(List.of(sparkInput("cat", "ns.src")), List.of()));

    assertThat(result.dispositions().values()).containsExactly(Disposition.AUTHORIZED);
  }

  @Test
  void anUnreadableInputIsOmittedWhileAWritableOutputIsAuthorized() {
    // THE FORGERY VECTOR. A caller who can write cat.ns.dst must not be able to assert that dst was
    // derived from cat.ns.secret, a table they cannot even read. The output stands; the input does
    // not, so no edge involving secret can be persisted.
    catalog.tableExists("cat", "ns", "secret");
    catalog.tableExists("cat", "ns", "dst");
    catalog.grant(INGEST_TABLE_LINEAGE, table("cat", "ns", "dst"));
    // Deliberately no REFERENCE_LINEAGE_INPUT_TABLE on secret.

    LineageAuthorizationResult result =
        ingestAuthorizer.authorize(
            runEvent(
                List.of(sparkInput("cat", "ns.secret")), List.of(sparkOutput("cat", "ns.dst"))));

    assertThat(result.dispositions().values())
        .containsExactly(Disposition.OMITTED, Disposition.AUTHORIZED);
    assertThat(result.isNoOp()).isFalse();
    assertThat(result.requiresFiltering()).isTrue();
    assertThat(result.omittedCount()).isEqualTo(1);
  }

  @Test
  void aMissingDatasetAndAnUnauthorizedOneAreIndistinguishable() {
    // A caller walking candidate names must not be able to read existence off the result.
    catalog.tableExists("cat", "ns", "denied");

    LineageAuthorizationResult result =
        ingestAuthorizer.authorize(
            runEvent(
                List.of(sparkInput("cat", "ns.denied"), sparkInput("cat", "ns.missing")),
                List.of()));

    assertThat(result.dispositions().values()).containsOnly(Disposition.OMITTED);
    assertThat(result.omittedCount()).isEqualTo(2);
  }

  // ---------------------------------------------------------------------------------------------
  // Tables and views.
  // ---------------------------------------------------------------------------------------------

  @Test
  void aViewInputNeedsTheViewReferencePrivilege() {
    catalog.viewExists("cat", "ns", "v");
    catalog.grant(REFERENCE_LINEAGE_INPUT_VIEW, table("cat", "ns", "v"));

    LineageAuthorizationResult result =
        ingestAuthorizer.authorize(runEvent(List.of(sparkInput("cat", "ns.v")), List.of()));

    assertThat(result.dispositions().values()).containsExactly(Disposition.AUTHORIZED);
    assertThat(operationOf(catalog.authorizeRequests.get(0)))
        .isEqualTo(REFERENCE_LINEAGE_INPUT_VIEW);
  }

  @Test
  void aTableGrantDoesNotAuthorizeAView() {
    catalog.viewExists("cat", "ns", "v");
    catalog.grant(REFERENCE_LINEAGE_INPUT_TABLE, table("cat", "ns", "v"));
    catalog.grant(INGEST_TABLE_LINEAGE, table("cat", "ns", "v"));

    LineageAuthorizationResult result =
        ingestAuthorizer.authorize(
            runEvent(List.of(sparkInput("cat", "ns.v")), List.of(sparkOutput("cat", "ns.v"))));

    assertThat(result.dispositions().values()).containsOnly(Disposition.OMITTED);
  }

  @ParameterizedTest
  @MethodSource("ingestOperationBySubtype")
  void anOutputNeedsTheIngestPrivilegeOfWhatItResolvedTo(
      boolean view, PolarisAuthorizableOperation expected) {
    if (view) {
      catalog.viewExists("cat", "ns", "dst");
    } else {
      catalog.tableExists("cat", "ns", "dst");
    }
    catalog.grant(expected, table("cat", "ns", "dst"));

    LineageAuthorizationResult result =
        ingestAuthorizer.authorize(runEvent(List.of(), List.of(sparkOutput("cat", "ns.dst"))));

    assertThat(result.dispositions().values()).containsExactly(Disposition.AUTHORIZED);
    assertThat(operationOf(catalog.authorizeRequests.get(0))).isEqualTo(expected);
  }

  static Stream<Arguments> ingestOperationBySubtype() {
    return Stream.of(
        Arguments.of(false, INGEST_TABLE_LINEAGE), Arguments.of(true, INGEST_VIEW_LINEAGE));
  }

  // ---------------------------------------------------------------------------------------------
  // Output fallback and the roles that have none.
  // ---------------------------------------------------------------------------------------------

  @Test
  void anOutputTableThatDoesNotExistYetFallsBackToItsParentNamespace() {
    // Create-table-as-select: a START event legitimately names a table that does not exist yet.
    catalog.namespaceExists("cat", "ns");
    catalog.grant(INGEST_TABLE_LINEAGE, namespace("cat", "ns"));

    LineageAuthorizationResult result =
        ingestAuthorizer.authorize(runEvent(List.of(), List.of(sparkOutput("cat", "ns.fresh"))));

    assertThat(result.dispositions().values()).containsExactly(Disposition.AUTHORIZED);
  }

  @Test
  void aNamespaceFallbackWithoutTheNamespaceGrantIsOmitted() {
    catalog.namespaceExists("cat", "ns");

    LineageAuthorizationResult result =
        ingestAuthorizer.authorize(runEvent(List.of(), List.of(sparkOutput("cat", "ns.fresh"))));

    assertThat(result.dispositions().values()).containsExactly(Disposition.OMITTED);
  }

  @Test
  void anOutputTableThatExistsUsesTheTableGrantAndNotTheNamespaceFallback() {
    // The caller holds ingest on the namespace but not on this existing table. Falling back would
    // let a namespace grant stand in for a table the caller was specifically not given.
    catalog.tableExists("cat", "ns", "dst");
    catalog.grant(INGEST_TABLE_LINEAGE, namespace("cat", "ns"));

    LineageAuthorizationResult result =
        ingestAuthorizer.authorize(runEvent(List.of(), List.of(sparkOutput("cat", "ns.dst"))));

    assertThat(result.dispositions().values()).containsExactly(Disposition.OMITTED);
  }

  @Test
  void anOutputWhoseTableAndNamespaceBothFailToResolveIsOmitted() {
    catalog.catalogExists("cat");

    LineageAuthorizationResult result =
        ingestAuthorizer.authorize(runEvent(List.of(), List.of(sparkOutput("cat", "ns.dst"))));

    assertThat(result.dispositions().values()).containsExactly(Disposition.OMITTED);
    assertThat(catalog.authorizeRequests).isEmpty();
  }

  @Test
  void anInputThatDoesNotResolveNeverFallsBackToItsNamespace() {
    catalog.namespaceExists("cat", "ns");
    catalog.grant(INGEST_TABLE_LINEAGE, namespace("cat", "ns"));
    catalog.grant(REFERENCE_LINEAGE_INPUT_TABLE, namespace("cat", "ns"));

    LineageAuthorizationResult result =
        ingestAuthorizer.authorize(runEvent(List.of(sparkInput("cat", "ns.typo")), List.of()));

    assertThat(result.dispositions().values()).containsExactly(Disposition.OMITTED);
    assertThat(catalog.authorizeRequests).isEmpty();
    assertThat(catalog.registeredPathKeys("cat")).containsExactly(tableKey("ns", "typo"));
  }

  @Test
  void aStandaloneDatasetThatDoesNotResolveNeverFallsBackToItsNamespace() {
    catalog.namespaceExists("cat", "ns");
    catalog.grant(INGEST_TABLE_LINEAGE, namespace("cat", "ns"));

    LineageAuthorizationResult result =
        ingestAuthorizer.authorize(
            datasetEvent(
                staticDatasetWithFacets(
                    "s3://bucket", "wh/ns/fresh", sparkFacets(CATALOG_URI, "cat", "ns.fresh"))));

    assertThat(result.dispositions().values()).containsExactly(Disposition.OMITTED);
    assertThat(catalog.authorizeRequests).isEmpty();
  }

  @Test
  void aStandaloneDatasetThatResolvesNeedsIngestOnTheTable() {
    catalog.tableExists("cat", "ns", "t");
    catalog.grant(INGEST_TABLE_LINEAGE, table("cat", "ns", "t"));

    LineageAuthorizationResult result =
        ingestAuthorizer.authorize(
            datasetEvent(
                staticDatasetWithFacets(
                    "s3://bucket", "wh/ns/t", sparkFacets(CATALOG_URI, "cat", "ns.t"))));

    assertThat(result.dispositions().values()).containsExactly(Disposition.AUTHORIZED);
  }

  @Test
  void oneTableAsBothInputAndOutputGetsTwoIndependentDecisions() {
    // A MERGE INTO reads and writes the same table. Read-only access must authorize the input and
    // not the output.
    catalog.tableExists("cat", "ns", "t");
    catalog.grant(REFERENCE_LINEAGE_INPUT_TABLE, table("cat", "ns", "t"));

    LineageAuthorizationResult result =
        ingestAuthorizer.authorize(
            runEvent(List.of(sparkInput("cat", "ns.t")), List.of(sparkOutput("cat", "ns.t"))));

    assertThat(result.dispositions().values())
        .containsExactly(Disposition.AUTHORIZED, Disposition.OMITTED);
  }

  // ---------------------------------------------------------------------------------------------
  // Whether the dataset is ours at all.
  // ---------------------------------------------------------------------------------------------

  @Test
  void datasetsWithoutACatalogAreExternalAndNeedNoAuthorization() {
    catalog.tableExists("cat", "ns", "dst");
    catalog.grant(INGEST_TABLE_LINEAGE, table("cat", "ns", "dst"));

    LineageAuthorizationResult result =
        ingestAuthorizer.authorize(
            runEvent(
                List.of(input("kafka://broker", "topic")), List.of(sparkOutput("cat", "ns.dst"))));

    assertThat(result.dispositions().values())
        .containsExactly(Disposition.EXTERNAL, Disposition.AUTHORIZED);
    assertThat(result.requiresFiltering()).isFalse();
    assertThat(catalog.authorizeRequests).hasSize(1);
    assertThat(catalog.manifestsCreatedFor).containsExactly("cat");
  }

  @Test
  void anEventOfOnlyExternalDatasetsIsANoOpThatResolvesNothing() {
    LineageAuthorizationResult result =
        ingestAuthorizer.authorize(
            runEvent(
                List.of(input("kafka://broker", "topic")), List.of(output("s3://raw", "blob"))));

    assertThat(result.dispositions().values()).containsOnly(Disposition.EXTERNAL);
    assertThat(result.isNoOp()).isTrue();
    assertThat(catalog.manifestsCreatedFor).isEmpty();
  }

  @Test
  void aCatalogThatIsNotInTheRealmMakesItsDatasetsExternalWithoutThrowing() {
    // The catalog is named by the event but is another server's. Resolution fails without throwing
    // and nothing is authorized, whether or not a table of that name could exist.
    var event =
        runEvent(
            List.of(sparkInput("elsewhere", "ns.a")), List.of(sparkOutput("elsewhere", "ns.b")));

    assertThatCode(() -> ingestAuthorizer.authorize(event)).doesNotThrowAnyException();

    LineageAuthorizationResult result = ingestAuthorizer.authorize(event);

    assertThat(result.dispositions().values()).containsOnly(Disposition.EXTERNAL);
    assertThat(result.isNoOp()).isTrue();
    assertThat(catalog.authorizeRequests).isEmpty();
  }

  @Test
  void whetherTheCatalogIsOursDoesNotDependOnTheTableExisting() {
    // The catalog resolves but the output does not exist yet: still ours, so still omitted rather
    // than recorded as an unauthenticated external node.
    catalog.catalogExists("cat");

    LineageAuthorizationResult result =
        ingestAuthorizer.authorize(runEvent(List.of(), List.of(sparkOutput("cat", "ns.fresh"))));

    assertThat(result.dispositions().values()).containsExactly(Disposition.OMITTED);
    assertThat(result.externalCount()).isZero();
  }

  @Test
  void anUnaddressableClaimOnOurCatalogIsOmittedAndNeverAuthorized() {
    // The name has no namespace, so it cannot be turned into a table. It must not be recorded as an
    // external node beside the real cat.ns.dst.
    catalog.tableExists("cat", "ns", "dst");
    catalog.grant(INGEST_TABLE_LINEAGE, table("cat", "ns", "dst"));

    LineageAuthorizationResult result =
        ingestAuthorizer.authorize(
            runEvent(
                List.of(
                    inputWithFacets(
                        "s3://bucket", "wh/dst", sparkFacets(CATALOG_URI, "cat", "dst"))),
                List.of(sparkOutput("cat", "ns.dst"))));

    assertThat(result.dispositions().values())
        .containsExactly(Disposition.OMITTED, Disposition.AUTHORIZED);
    // Only the real output contributed paths (its table plus its namespace fallback).
    assertThat(catalog.registeredPathKeys("cat"))
        .containsExactlyInAnyOrder(tableKey("ns", "dst"), namespaceKey("ns"));
    assertThat(catalog.authorizeRequests).hasSize(1);
  }

  @Test
  void anUnaddressableClaimOnACatalogNotInTheRealmIsExternal() {
    LineageAuthorizationResult result =
        ingestAuthorizer.authorize(
            runEvent(
                List.of(
                    inputWithFacets(
                        "s3://bucket", "wh/dst", sparkFacets(CATALOG_URI, "elsewhere", "dst"))),
                List.of()));

    assertThat(result.dispositions().values()).containsExactly(Disposition.EXTERNAL);
  }

  // ---------------------------------------------------------------------------------------------
  // Resolution strategy invariants.
  // ---------------------------------------------------------------------------------------------

  @Test
  void everyRegisteredPathIsOptional() {
    // Load-bearing: one non-optional miss fails resolveAll, after which every lookup returns null
    // and the authorization intent trips a precondition, turning a drop into a 500.
    ingestAuthorizer.authorize(
        runEvent(List.of(sparkInput("cat", "ns.a")), List.of(sparkOutput("cat", "ns.b"))));

    assertThat(catalog.registeredPaths.get("cat")).isNotEmpty().allMatch(p -> p.optional());
  }

  @Test
  void theNamespacePathIsRegisteredForOutputsOnly() {
    ingestAuthorizer.authorize(runEvent(List.of(sparkInput("cat", "ns.a")), List.of()));
    assertThat(catalog.registeredPathKeys("cat")).containsExactly(tableKey("ns", "a"));

    FakeLineageCatalog other = new FakeLineageCatalog();
    other.ingestAuthorizer().authorize(runEvent(List.of(), List.of(sparkOutput("cat", "ns.b"))));
    assertThat(other.registeredPathKeys("cat"))
        .containsExactlyInAnyOrder(tableKey("ns", "b"), namespaceKey("ns"));
  }

  @Test
  void pathsAreReadBackWithTheSameRootingTheAuthorizerUses() {
    catalog.tableExists("cat", "ns", "a");

    ingestAuthorizer.authorize(runEvent(List.of(sparkInput("cat", "ns.a")), List.of()));

    assertThat(catalog.prependRootContainerFlags).isNotEmpty().containsOnly(true);
  }

  @Test
  void resolutionHappensExactlyOncePerCatalog() {
    ingestAuthorizer.authorize(
        runEvent(
            List.of(sparkInput("cat", "ns.a"), sparkInput("cat", "ns.b")),
            List.of(sparkOutput("cat", "ns.c"))));

    assertThat(catalog.resolveRequests).hasSize(1);
    assertThat(catalog.manifestsCreatedFor).containsExactly("cat");
  }

  @Test
  void eachDistinctCatalogGetsItsOwnManifest() {
    ingestAuthorizer.authorize(
        runEvent(
            List.of(sparkInput("one", "ns.a"), sparkInput("two", "ns.a")),
            List.of(sparkOutput("one", "ns.b"))));

    assertThat(catalog.manifestsCreatedFor).containsExactly("one", "two");
    assertThat(catalog.resolveRequests).hasSize(2);
  }

  @Test
  void repeatedPathKeysAreRegisteredOnlyOnce() {
    ingestAuthorizer.authorize(
        runEvent(
            List.of(sparkInput("cat", "ns.t")),
            List.of(sparkOutput("cat", "ns.t"), sparkOutput("cat", "ns.u"))));

    assertThat(catalog.registeredPathKeys("cat"))
        .containsExactlyInAnyOrder(tableKey("ns", "t"), tableKey("ns", "u"), namespaceKey("ns"));
  }

  @Test
  void theResolutionHintCarriesBothTheTableAndTheViewOperationForEveryDataset() {
    // The event does not say table or view, and a request-selective authorizer may decide what to
    // resolve from the operation, so the hint must cover whichever the entity turns out to be.
    ingestAuthorizer.authorize(
        runEvent(List.of(sparkInput("cat", "ns.a")), List.of(sparkOutput("cat", "ns.b"))));

    assertThat(catalog.resolveRequests).hasSize(1);
    assertThat(
            catalog.resolveRequests.get(0).intents().stream()
                .map(intent -> ((SingleTargetAuthorizationIntent) intent).operation()))
        .containsExactlyInAnyOrder(
            REFERENCE_LINEAGE_INPUT_TABLE,
            REFERENCE_LINEAGE_INPUT_VIEW,
            INGEST_TABLE_LINEAGE,
            INGEST_VIEW_LINEAGE,
            INGEST_TABLE_LINEAGE);
  }

  // ---------------------------------------------------------------------------------------------
  // Event-level outcomes.
  // ---------------------------------------------------------------------------------------------

  @Test
  void anEventWhereEverythingIsDeniedIsANoOp() {
    catalog.tableExists("cat", "ns", "a");

    LineageAuthorizationResult result =
        ingestAuthorizer.authorize(runEvent(List.of(sparkInput("cat", "ns.a")), List.of()));

    assertThat(result.isNoOp()).isTrue();
  }

  @Test
  void anEventThatReferencesNoDatasetsIsNotANoOp() {
    // Job-lifecycle traffic makes no claim about any entity, so there is no edge to forge.
    LineageAuthorizationResult result = ingestAuthorizer.authorize(runEvent(List.of(), List.of()));

    assertThat(result.isNoOp()).isFalse();
    assertThat(result.requiresFiltering()).isFalse();
  }

  @Test
  void anEventOfUnenumerableShapeIsANoOp() {
    assertThat(ingestAuthorizer.authorize(null).isNoOp()).isTrue();
  }
}
