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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.auth.AuthorizationDecision;
import org.apache.polaris.core.auth.AuthorizationIntent;
import org.apache.polaris.core.auth.AuthorizationRequest;
import org.apache.polaris.core.auth.AuthorizationState;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.PolarisSecurable;
import org.apache.polaris.core.auth.SingleTargetAuthorizationIntent;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.service.catalog.common.PolarisSecurableMapper;
import org.apache.polaris.service.lineage.LineageAuthorizationResult.Disposition;
import org.apache.polaris.service.lineage.LineageDatasetKey.Role;
import org.apache.polaris.service.lineage.PolarisDatasetIdentifier.Identification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Authorizes the datasets an OpenLineage event references, classifying each one independently.
 *
 * <p>Lineage ingest cannot be authorized the way an ordinary catalog operation is, because an event
 * is not one operation on one target. It is an assertion about an edge — "this job read A and wrote
 * B" — where each endpoint may be a Polaris table, a table in a different catalog, or something
 * entirely outside Polaris, and where the privilege required differs by endpoint. Checking the
 * event as a unit would either fail the whole event because one endpoint is not the caller's, or
 * authorize the whole event on the strength of the one endpoint that is. Neither is acceptable: the
 * second is the forgery vector, in which a caller who can write table B asserts that B was derived
 * from a table A they cannot even read, and thereby writes a false provenance claim about A into
 * the graph.
 *
 * <p>So each dataset gets its own decision, and the ones that do not pass are removed from the
 * event before the provider sees it. The provider is handed a rebuilt event containing only
 * authorized and external datasets, rather than the original plus advisory metadata, so that a
 * provider which ignores the contract entirely still cannot persist a forged edge.
 *
 * <p>Required privilege by role:
 *
 * <ul>
 *   <li>{@code INPUT} — the table must resolve, and {@code REFERENCE_LINEAGE_INPUT_TABLE} must be
 *       allowed on it. There is deliberately <em>no</em> namespace fallback: an input that does not
 *       exist is a typo or something external, and letting it fall back would turn a
 *       namespace-level grant into "you may assert provenance from any name under this namespace",
 *       which is exactly the claim the per-dataset check exists to prevent.
 *   <li>{@code OUTPUT} — the table must resolve and allow {@code INGEST_LINEAGE}; otherwise the
 *       parent namespace must resolve and allow it instead. The fallback exists for
 *       create-table-as-select, where the event legitimately describes a table that does not exist
 *       yet, and it is safe here because the caller is claiming to have <em>created</em> something
 *       under a namespace they hold ingest on, not claiming a fact about someone else's data.
 *   <li>{@code STANDALONE} — the table must resolve and allow {@code INGEST_LINEAGE}. No fallback,
 *       for the same reason as {@code INPUT}: a {@code DatasetEvent} about a nonexistent table
 *       asserts nothing a namespace grant covers.
 * </ul>
 *
 * <h2>Resolution strategy</h2>
 *
 * <p>Candidate paths are registered on a shared manifest per catalog and resolved in one pass, so
 * an event with N datasets costs one resolution round per distinct catalog rather than N. Five
 * constraints shape how that works:
 *
 * <ul>
 *   <li><strong>Every path is optional.</strong> An optional miss leaves a short resolved path that
 *       reads back as null while its siblings resolve normally, which is exactly the per-dataset
 *       behaviour this pass needs. A <em>non</em>-optional miss instead fails the whole resolution,
 *       after which every lookup returns null — turning one nonexistent dataset into a total loss.
 *   <li><strong>Resolution goes through the authorizer, never {@code resolveAll()}
 *       directly.</strong> {@link PolarisAuthorizer#resolveAuthorizationInputs} is the SPI hook
 *       that triggers it, and the request handed to it must carry the <em>union</em> of every
 *       intent that may later be authorized from the resulting state: a request-selective
 *       authorizer may use the operation, not just the target securable, to decide what inputs to
 *       resolve.
 *   <li><strong>One manifest per catalog, and never a null one.</strong> A manifest binds one
 *       reference catalog at construction and resolves once; with a null reference catalog the
 *       resolver skips requested paths entirely, so a null-catalog manifest cannot resolve paths at
 *       all. Datasets are therefore grouped by catalog first.
 *   <li><strong>One intent per authorize call.</strong> {@link PolarisAuthorizer#authorize} defines
 *       a multi-intent request as a single AND-combined batch that may short-circuit on the first
 *       deny. That yields one bit for the whole batch; this pass needs N independent bits, so it
 *       issues N single-intent requests against the one resolved state.
 *   <li><strong>Resolution is probed before any decision is requested.</strong> An unresolved
 *       target fails a precondition inside the intent resolver, which escapes as a 500 rather than
 *       a deny, so a dataset is classified unresolved without ever reaching {@code authorize}.
 * </ul>
 *
 * <p>{@code addPassthroughPath} is deliberately not used: it builds a fresh single-use resolver per
 * call, which would make the pass genuinely O(N) resolutions and defeat the batching.
 */
public final class LineageIngestAuthorizer {

  private static final Logger LOGGER = LoggerFactory.getLogger(LineageIngestAuthorizer.class);

  private final PolarisPrincipal principal;
  private final ResolutionManifestFactory resolutionManifestFactory;
  private final PolarisAuthorizer authorizer;
  private final PolarisDatasetIdentifier datasetIdentifier;

  public LineageIngestAuthorizer(
      PolarisPrincipal principal,
      ResolutionManifestFactory resolutionManifestFactory,
      PolarisAuthorizer authorizer,
      PolarisDatasetIdentifier datasetIdentifier) {
    this.principal = principal;
    this.resolutionManifestFactory = resolutionManifestFactory;
    this.authorizer = authorizer;
    this.datasetIdentifier = datasetIdentifier;
  }

  /** Classifies every dataset {@code event} references. */
  public LineageAuthorizationResult authorize(OpenLineage.BaseEvent event) {
    if (!LineageDatasetExtractor.isEnumerable(event)) {
      LOGGER.info(
          "Not ingesting lineage event of unenumerable shape {}: its dataset references cannot be"
              + " authorized",
          event == null ? "null" : event.getClass().getName());
      return LineageAuthorizationResult.notEnumerable();
    }

    List<LineageDatasetKey> datasets = LineageDatasetExtractor.extract(event);
    if (datasets.isEmpty()) {
      return LineageAuthorizationResult.of(Map.of());
    }

    // Dispositions are seeded in encounter order so the reported order matches the event.
    Map<LineageDatasetKey, Disposition> dispositions = new LinkedHashMap<>();
    Map<String, List<Candidate>> candidatesByCatalog = new LinkedHashMap<>();

    for (LineageDatasetKey dataset : datasets) {
      Identification identification =
          datasetIdentifier.identify(dataset.namespace(), dataset.name());
      switch (identification.kind()) {
        case EXTERNAL -> dispositions.put(dataset, Disposition.EXTERNAL);
        case UNADDRESSABLE_POLARIS_CLAIM ->
            // Claimed a Polaris catalog but is not an addressable table path. Treated exactly like
            // a Polaris table that does not exist, rather than recorded as an external node.
            dispositions.put(dataset, Disposition.DROPPED_UNRESOLVED);
        case POLARIS ->
            candidatesByCatalog
                .computeIfAbsent(identification.identity().catalog(), catalog -> new ArrayList<>())
                .add(new Candidate(dataset, identification.identity()));
      }
    }

    candidatesByCatalog.forEach(
        (catalog, candidates) -> classifyCatalog(catalog, candidates, dispositions));

    return LineageAuthorizationResult.of(dispositions);
  }

  /**
   * Resolves and classifies every candidate in one catalog, using a single manifest and a single
   * resolution pass.
   */
  private void classifyCatalog(
      String catalog,
      List<Candidate> candidates,
      Map<LineageDatasetKey, Disposition> dispositions) {
    PolarisResolutionManifest manifest =
        resolutionManifestFactory.createResolutionManifest(principal, catalog);

    // Registering the same lookup key twice is last-write-wins on the manifest but still costs a
    // resolver path, so keys are deduplicated here.
    Set<ResolvedPathKey> registered = new HashSet<>();
    List<AuthorizationIntent> candidateIntents = new ArrayList<>();
    for (Candidate candidate : candidates) {
      PolarisDatasetIdentity identity = candidate.identity();
      register(manifest, registered, tablePath(identity));
      candidateIntents.add(
          new SingleTargetAuthorizationIntent(
              tableOperation(candidate.role()), tableSecurable(identity)));
      if (candidate.role() == Role.OUTPUT) {
        register(manifest, registered, namespacePath(identity));
        candidateIntents.add(
            new SingleTargetAuthorizationIntent(
                PolarisAuthorizableOperation.INGEST_LINEAGE, namespaceSecurable(identity)));
      }
    }

    AuthorizationState authorizationState = new AuthorizationState(manifest);

    // Resolve every intent that may be authorized from this shared AuthorizationState, in one pass.
    // A request-selective authorizer may use the operation, not just the target securable, to
    // decide
    // what inputs to resolve, so the resolution request must carry the union of the candidate
    // intents — including each output's namespace fallback — even though only one of them is later
    // used per dataset. This mirrors CatalogHandler's registerTableOverwrite flow. It is also the
    // only resolution pass available: a manifest resolves exactly once, and this is the hook that
    // triggers it, so resolveAll() is never called directly. A catalog that does not exist fails
    // resolution here without throwing, correctly leaving every candidate in it unresolved rather
    // than failing the request.
    authorizer.resolveAuthorizationInputs(
        authorizationState, new AuthorizationRequest(principal, candidateIntents));

    for (Candidate candidate : candidates) {
      Disposition disposition = classify(authorizationState, manifest, candidate);
      if (disposition != Disposition.AUTHORIZED) {
        // Specifics stay server-side; only aggregate counts are returned to the caller.
        LOGGER.info(
            "Omitting {} dataset {} from lineage event for principal {}: {}",
            candidate.role(),
            candidate.identity(),
            principal == null ? "unknown" : principal.getName(),
            disposition);
      }
      dispositions.put(candidate.dataset(), disposition);
    }
  }

  /**
   * Decides one candidate: resolve first, and only ask for a privilege decision on something that
   * resolved.
   *
   * <p>The order matters. {@code AuthorizationIntentResolver} requires an intent's target to have
   * resolved and fails a precondition if it did not, so handing it an unresolved target would raise
   * a 500 rather than produce a deny. Probing the manifest first converts that into a clean {@code
   * DROPPED_UNRESOLVED}.
   */
  private Disposition classify(
      AuthorizationState state, PolarisResolutionManifest manifest, Candidate candidate) {
    PolarisDatasetIdentity identity = candidate.identity();

    if (isResolved(manifest, tablePathKey(identity))) {
      return decide(state, tableOperation(candidate.role()), tableSecurable(identity));
    }

    if (candidate.role() != Role.OUTPUT) {
      // No namespace fallback for INPUT or STANDALONE — see the class javadoc. An input naming a
      // table that does not exist is a typo or an external dataset the identity rules misread; in
      // either case it is not something a namespace grant lets the caller assert.
      return Disposition.DROPPED_UNRESOLVED;
    }

    if (!isResolved(manifest, namespacePathKey(identity))) {
      return Disposition.DROPPED_UNRESOLVED;
    }

    // Create-table-as-select: the output table does not exist yet, so the claim is authorized
    // against the namespace it would be created in.
    return decide(state, PolarisAuthorizableOperation.INGEST_LINEAGE, namespaceSecurable(identity));
  }

  /**
   * Whether {@code key} fully resolved for this caller.
   *
   * <p>All four lineage operations register with the default {@code ResolvedPathRooting.ROOT}, so
   * {@code PolarisAuthorizerImpl} reads their paths back with {@code prependRootContainer = true};
   * this probe matches, as every read-back in {@code CatalogHandler} does. The flag does not change
   * the answer — {@code getResolvedPath} decides null before it consults the flag, which only
   * controls whether the root container is prepended to the returned path — but matching the
   * authorizer's own rooting keeps the probe and the decision reading the same view, and keeps the
   * returned wrapper correct if anything ever uses its contents rather than just its nullity.
   */
  private static boolean isResolved(PolarisResolutionManifest manifest, ResolvedPathKey key) {
    return manifest.getResolvedPath(key, true) != null;
  }

  private Disposition decide(
      AuthorizationState state, PolarisAuthorizableOperation operation, PolarisSecurable target) {
    AuthorizationRequest request =
        new AuthorizationRequest(
            principal, List.of(new SingleTargetAuthorizationIntent(operation, target)));
    AuthorizationDecision decision = authorizer.authorize(state, request);
    return decision.isAllowed() ? Disposition.AUTHORIZED : Disposition.DROPPED_UNAUTHORIZED;
  }

  /**
   * The paths are registered <em>optional</em>, which is load-bearing: a non-optional path that
   * fails to resolve fails the whole {@code resolveAll()}, after which every lookup on the manifest
   * returns null and the intent trips a precondition -- turning a dataset that merely does not
   * exist into a 500 rather than a per-dataset drop. Each path and the key it is read back with are
   * derived from one {@link ResolvedPathKey}, so they cannot drift apart.
   */
  private static ResolverPath tablePath(PolarisDatasetIdentity identity) {
    return new ResolverPath(tablePathKey(identity), true);
  }

  private static ResolverPath namespacePath(PolarisDatasetIdentity identity) {
    return new ResolverPath(namespacePathKey(identity), true);
  }

  private static ResolvedPathKey tablePathKey(PolarisDatasetIdentity identity) {
    return ResolvedPathKey.ofTableLike(identity.table());
  }

  private static ResolvedPathKey namespacePathKey(PolarisDatasetIdentity identity) {
    return ResolvedPathKey.ofNamespace(identity.table().namespace());
  }

  private static PolarisSecurable tableSecurable(PolarisDatasetIdentity identity) {
    return PolarisSecurableMapper.tableLike(identity.catalog(), identity.table());
  }

  private static PolarisSecurable namespaceSecurable(PolarisDatasetIdentity identity) {
    return PolarisSecurableMapper.namespace(identity.catalog(), identity.table().namespace());
  }

  private static void register(
      PolarisResolutionManifest manifest, Set<ResolvedPathKey> registered, ResolverPath path) {
    if (registered.add(path.key())) {
      manifest.addPath(path);
    }
  }

  /**
   * Inputs are checked for read access to the table; outputs and standalone datasets are checked
   * for ingest. Ingest is one operation whether the target is the table or, for
   * create-table-as-select, its parent namespace — the securable carries that distinction.
   */
  private static PolarisAuthorizableOperation tableOperation(Role role) {
    return role == Role.INPUT
        ? PolarisAuthorizableOperation.REFERENCE_LINEAGE_INPUT_TABLE
        : PolarisAuthorizableOperation.INGEST_LINEAGE;
  }

  /** One dataset occurrence that named an addressable Polaris table, paired with that identity. */
  private record Candidate(LineageDatasetKey dataset, PolarisDatasetIdentity identity) {
    Role role() {
      return dataset.role();
    }
  }
}
