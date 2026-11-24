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

import static org.apache.polaris.service.lineage.LineageTestEvents.inputWithFacets;
import static org.apache.polaris.service.lineage.LineageTestEvents.outputWithFacets;
import static org.apache.polaris.service.lineage.LineageTestEvents.sparkFacets;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.server.OpenLineage;
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
import org.apache.polaris.core.auth.AuthorizationState;
import org.apache.polaris.core.auth.PathSegment;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.PolarisSecurable;
import org.apache.polaris.core.auth.SingleTargetAuthorizationIntent;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.apache.polaris.service.catalog.common.PolarisSecurableMapper;

/**
 * A stand-in for the realm's catalogs and the caller's grants, driving a real {@link
 * LineageIngestAuthorizer} without a persistence layer.
 *
 * <p>It holds the catalogs that exist, the path keys that resolve in each with the subtype of the
 * entity they resolve to, and the granted {@code (operation, securable)} pairs. A test can say
 * "this table exists and the caller may read it" and assert the resulting disposition. The fake
 * manifest enforces the real manifest's contract that a lookup key must have been registered before
 * it is queried, so a missing {@code addPath} fails the test rather than silently reading as
 * unresolved, and it reports a failed resolution for a catalog that does not exist.
 */
final class FakeLineageCatalog {

  /** The {@code uri} of the Iceberg REST catalog the events say they were loaded from. */
  static final String CATALOG_URI = "http://polaris:8181/api/catalog";

  private final Set<String> catalogs = new HashSet<>();
  private final Map<String, Map<ResolvedPathKey, PolarisEntitySubType>> resolvable =
      new HashMap<>();
  private final Set<String> grants = new HashSet<>();

  final Map<String, List<ResolverPath>> registeredPaths = new LinkedHashMap<>();
  final List<String> manifestsCreatedFor = new ArrayList<>();
  final List<AuthorizationRequest> resolveRequests = new ArrayList<>();
  final List<AuthorizationRequest> authorizeRequests = new ArrayList<>();
  final List<Boolean> prependRootContainerFlags = new ArrayList<>();

  final PolarisPrincipal principal = mock(PolarisPrincipal.class);
  final ResolutionManifestFactory manifestFactory = mock(ResolutionManifestFactory.class);
  final PolarisAuthorizer authorizer = mock(PolarisAuthorizer.class);

  FakeLineageCatalog() {
    when(principal.getName()).thenReturn("alice");
    when(manifestFactory.createResolutionManifest(any(), nullable(String.class)))
        .thenAnswer(invocation -> {
          String catalog = invocation.getArgument(1);
          manifestsCreatedFor.add(catalog);
          return fakeManifest(catalog);
        });
    doAnswer(
            invocation -> {
              resolveRequests.add(invocation.getArgument(1));
              return null;
            })
        .when(authorizer)
        .resolveAuthorizationInputs(any(AuthorizationState.class), any(AuthorizationRequest.class));
    when(authorizer.authorize(any(AuthorizationState.class), any(AuthorizationRequest.class)))
        .thenAnswer(invocation -> {
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
  }

  LineageIngestAuthorizer ingestAuthorizer() {
    return new LineageIngestAuthorizer(principal, manifestFactory, authorizer);
  }

  // ---------------------------------------------------------------------------------------------
  // What exists and what is granted.
  // ---------------------------------------------------------------------------------------------

  void catalogExists(String catalog) {
    catalogs.add(catalog);
  }

  void tableExists(String catalog, String namespace, String table) {
    entityExists(
        catalog,
        ResolvedPathKey.ofTableLike(TableIdentifier.of(Namespace.of(namespace), table)),
        PolarisEntitySubType.ICEBERG_TABLE);
  }

  void viewExists(String catalog, String namespace, String view) {
    entityExists(
        catalog,
        ResolvedPathKey.ofTableLike(TableIdentifier.of(Namespace.of(namespace), view)),
        PolarisEntitySubType.ICEBERG_VIEW);
  }

  void namespaceExists(String catalog, String... levels) {
    entityExists(
        catalog,
        ResolvedPathKey.ofNamespace(Namespace.of(levels)),
        PolarisEntitySubType.NULL_SUBTYPE);
  }

  void grant(PolarisAuthorizableOperation operation, PolarisSecurable securable) {
    grants.add(grantKey(operation, securable));
  }

  private void entityExists(String catalog, ResolvedPathKey key, PolarisEntitySubType subType) {
    catalogExists(catalog);
    resolvable.computeIfAbsent(catalog, c -> new HashMap<>()).put(key, subType);
  }

  // ---------------------------------------------------------------------------------------------
  // Securables and keys, as the authorizer is expected to build them.
  // ---------------------------------------------------------------------------------------------

  static PolarisSecurable table(String catalog, String namespace, String table) {
    return PolarisSecurableMapper.tableLike(
        catalog, TableIdentifier.of(Namespace.of(namespace), table));
  }

  static PolarisSecurable namespace(String catalog, String namespace) {
    return PolarisSecurableMapper.namespace(catalog, Namespace.of(namespace));
  }

  static ResolvedPathKey tableKey(String namespace, String table) {
    return ResolvedPathKey.ofTableLike(TableIdentifier.of(Namespace.of(namespace), table));
  }

  static ResolvedPathKey namespaceKey(String... levels) {
    return ResolvedPathKey.ofNamespace(Namespace.of(levels));
  }

  List<ResolvedPathKey> registeredPathKeys(String catalog) {
    return registeredPaths.getOrDefault(catalog, List.of()).stream()
        .map(ResolverPath::key)
        .toList();
  }

  static PolarisAuthorizableOperation operationOf(AuthorizationRequest request) {
    return ((SingleTargetAuthorizationIntent) request.intents().get(0)).operation();
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

  // ---------------------------------------------------------------------------------------------
  // Datasets as OpenLineage Spark emits them for a table in an Iceberg REST catalog: named by
  // storage location, with the catalog and the table's name in the facets.
  // ---------------------------------------------------------------------------------------------

  /** An input read from {@code catalog}'s table, named in Spark's {@code ns.tbl} form. */
  static OpenLineage.InputDataset sparkInput(String catalog, String qualifiedTable) {
    return inputWithFacets(
        "s3://bucket",
        "wh/" + qualifiedTable.replace('.', '/'),
        sparkFacets(CATALOG_URI, catalog, qualifiedTable));
  }

  /** An output written to {@code catalog}'s table, named in Spark's {@code ns.tbl} form. */
  static OpenLineage.OutputDataset sparkOutput(String catalog, String qualifiedTable) {
    return outputWithFacets(
        "s3://bucket",
        "wh/" + qualifiedTable.replace('.', '/'),
        sparkFacets(CATALOG_URI, catalog, qualifiedTable));
  }

  // ---------------------------------------------------------------------------------------------
  // The fake manifest.
  // ---------------------------------------------------------------------------------------------

  private PolarisResolutionManifest fakeManifest(String catalog) {
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    List<ResolverPath> paths = registeredPaths.computeIfAbsent(catalog, c -> new ArrayList<>());
    boolean catalogResolves = catalogs.contains(catalog);

    doAnswer(
            invocation -> {
              paths.add(invocation.getArgument(0));
              return null;
            })
        .when(manifest)
        .addPath(any());

    when(manifest.getPrimaryResolverStatusOrThrow())
        .thenReturn(
            catalogResolves
                ? new ResolverStatus(ResolverStatus.StatusEnum.SUCCESS)
                : new ResolverStatus(PolarisEntityType.CATALOG, catalog));

    when(manifest.getResolvedPath(any(ResolvedPathKey.class), anyBoolean()))
        .thenAnswer(invocation -> {
          ResolvedPathKey key = invocation.getArgument(0);
          prependRootContainerFlags.add(invocation.getArgument(1));
          // The real manifest fails a diagnostic check when a key was never registered, so a
          // missing addPath must fail the test rather than read as "unresolved".
          assertThat(paths).extracting(ResolverPath::key).contains(key);
          PolarisEntitySubType subType =
              catalogResolves ? resolvable.getOrDefault(catalog, Map.of()).get(key) : null;
          if (subType == null) {
            return null;
          }
          PolarisEntity leaf = mock(PolarisEntity.class);
          when(leaf.getSubType()).thenReturn(subType);
          PolarisResolvedPathWrapper wrapper = mock(PolarisResolvedPathWrapper.class);
          when(wrapper.getRawLeafEntity()).thenReturn(leaf);
          return wrapper;
        });

    return manifest;
  }
}
