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
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.auth.AuthorizationDecision;
import org.apache.polaris.core.auth.AuthorizationIntent;
import org.apache.polaris.core.auth.AuthorizationRequest;
import org.apache.polaris.core.auth.AuthorizationState;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.PolarisSecurable;
import org.apache.polaris.core.auth.SingleTargetAuthorizationIntent;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
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
 * <h2>Ours or external</h2>
 *
 * <p>{@link PolarisDatasetIdentifier} reads the catalog name out of the event but cannot tell
 * whether it is this server's. That is decided here by resolving the catalog in the realm: a
 * dataset is Polaris's if and only if its catalog resolves, whatever the state of the table. A
 * dataset whose catalog does not resolve is {@link Disposition#EXTERNAL}. The table is deliberately
 * <em>not</em> part of this decision, because OpenLineage sends events at every stage of a run and
 * a create-table-as-select {@code START} names an output that does not exist yet.
 *
 * <h2>Required privilege</h2>
 *
 * <p>Whether a resolved entity is a table or a view is not in the event, so the operation is chosen
 * from the subtype of the entity the name resolved to.
 *
 * <ul>
 *   <li>{@code INPUT} — the table or view must resolve, and {@code REFERENCE_LINEAGE_INPUT_TABLE}
 *       or {@code REFERENCE_LINEAGE_INPUT_VIEW} must be allowed on it. There is deliberately
 *       <em>no</em> namespace fallback: letting an input fall back would turn a namespace-level
 *       grant into "you may assert provenance from any name under this namespace", which is exactly
 *       the claim the per-dataset check exists to prevent.
 *   <li>{@code OUTPUT} — if the table or view resolves, {@code INGEST_TABLE_LINEAGE} or {@code
 *       INGEST_VIEW_LINEAGE} must be allowed on it. Otherwise the parent namespace must resolve and
 *       allow {@code INGEST_TABLE_LINEAGE}. The fallback exists for create-table-as-select, where
 *       the event legitimately describes a table that does not exist yet, and it is safe because
 *       the caller is claiming to have <em>created</em> something under a namespace they hold
 *       ingest on, not a fact about someone else's data.
 *   <li>{@code STANDALONE} — as {@code OUTPUT} but with no namespace fallback, for the same reason
 *       as {@code INPUT}: a {@code DatasetEvent} about a nonexistent table asserts nothing a
 *       namespace grant covers.
 * </ul>
 *
 * <p>A dataset that claims a resolving catalog but names no single usable table is dropped, and so
 * is one that does not exist or is not authorized. All three are one {@link Disposition#OMITTED}
 * outcome, so that a caller cannot tell a missing table from one they may not touch.
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
 *       after which every lookup returns null. It also means a successful resolution says the
 *       catalog exists, which is how the catalog is classified.
 *   <li><strong>Resolution goes through the authorizer, never {@code resolveAll()}
 *       directly.</strong> {@link PolarisAuthorizer#resolveAuthorizationInputs} is the SPI hook
 *       that triggers it, and the request handed to it must carry the <em>union</em> of every
 *       intent that may later be authorized from the resulting state: a request-selective
 *       authorizer may use the operation, not just the target securable, to decide what inputs to
 *       resolve. Both the table and the view operation are therefore included for each dataset.
 *   <li><strong>One manifest per catalog, and never a null one.</strong> A manifest binds one
 *       reference catalog at construction and resolves once. Datasets are grouped by catalog first.
 *   <li><strong>One intent per authorize call.</strong> {@link PolarisAuthorizer#authorize} defines
 *       a multi-intent request as a single AND-combined batch that may short-circuit on the first
 *       deny. That yields one bit for the whole batch; this pass needs N independent bits, so it
 *       issues N single-intent requests against the one resolved state.
 *   <li><strong>Resolution is probed before any decision is requested.</strong> An unresolved
 *       target fails a precondition inside the intent resolver, which escapes as a 500 rather than
 *       a deny, so a dataset is omitted without ever reaching {@code authorize}.
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

  public LineageIngestAuthorizer(
      PolarisPrincipal principal,
      ResolutionManifestFactory resolutionManifestFactory,
      PolarisAuthorizer authorizer) {
    this.principal = principal;
    this.resolutionManifestFactory = resolutionManifestFactory;
    this.authorizer = authorizer;
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
    Map<String, List<Claim>> claimsByCatalog = new LinkedHashMap<>();

    for (LineageDatasetKey dataset : datasets) {
      Identification identification = PolarisDatasetIdentifier.identify(dataset);
      if (identification.kind() == Identification.Kind.EXTERNAL) {
        dispositions.put(dataset, Disposition.EXTERNAL);
      } else {
        // A table of null marks an unaddressable claim on the catalog.
        claimsByCatalog
            .computeIfAbsent(identification.catalog(), catalog -> new ArrayList<>())
            .add(new Claim(dataset, identification.table()));
      }
    }

    claimsByCatalog.forEach((catalog, claims) -> classifyCatalog(catalog, claims, dispositions));

    return LineageAuthorizationResult.of(dispositions);
  }

  /**
   * Resolves and classifies every claim on one catalog, using a single manifest and a single
   * resolution pass.
   */
  private void classifyCatalog(
      String catalog, List<Claim> claims, Map<LineageDatasetKey, Disposition> dispositions) {
    PolarisResolutionManifest manifest =
        resolutionManifestFactory.createResolutionManifest(principal, catalog);

    // Registering the same lookup key twice is last-write-wins on the manifest but still costs a
    // resolver path, so keys are deduplicated here.
    Set<ResolvedPathKey> registered = new HashSet<>();
    List<AuthorizationIntent> candidateIntents = new ArrayList<>();
    for (Claim claim : claims) {
      if (claim.table() == null) {
        continue;
      }
      register(manifest, registered, tablePathKey(claim.table()));
      for (PolarisAuthorizableOperation operation : tableOperations(claim.role())) {
        candidateIntents.add(
            new SingleTargetAuthorizationIntent(operation, tableSecurable(catalog, claim.table())));
      }
      if (claim.role() == Role.OUTPUT) {
        register(manifest, registered, namespacePathKey(claim.table()));
        candidateIntents.add(
            new SingleTargetAuthorizationIntent(
                PolarisAuthorizableOperation.INGEST_TABLE_LINEAGE,
                namespaceSecurable(catalog, claim.table())));
      }
    }

    if (candidateIntents.isEmpty()) {
      // Only unaddressable claims name this catalog, so there is no table to resolve. The catalog
      // still has to be resolved to tell whether those claims are ours to omit or external, and a
      // request must carry an intent, so the hint targets the catalog itself.
      candidateIntents.add(
          new SingleTargetAuthorizationIntent(
              PolarisAuthorizableOperation.INGEST_TABLE_LINEAGE,
              PolarisSecurableMapper.catalog(catalog)));
    }

    AuthorizationState authorizationState = new AuthorizationState(manifest);

    // Resolve every intent that may be authorized from this shared AuthorizationState, in one pass.
    // A request-selective authorizer may use the operation, not just the target securable, to
    // decide what inputs to resolve, so the resolution request must carry the union of the
    // candidate intents -- both the table and the view operation, and each output's namespace
    // fallback -- even though only one of them is later used per dataset. This mirrors
    // CatalogHandler's registerTableOverwrite flow. It is also the only resolution pass available:
    // a manifest resolves exactly once, and this is the hook that triggers it, so resolveAll() is
    // never called directly.
    authorizer.resolveAuthorizationInputs(
        authorizationState, new AuthorizationRequest(principal, candidateIntents));

    // Every path is optional, so resolution only fails when the catalog itself is not in the realm.
    // Such a dataset is not ours to authorize and is recorded as external.
    if (manifest.getPrimaryResolverStatusOrThrow().getStatus()
        != ResolverStatus.StatusEnum.SUCCESS) {
      claims.forEach(claim -> dispositions.put(claim.dataset(), Disposition.EXTERNAL));
      return;
    }

    for (Claim claim : claims) {
      Disposition disposition = classify(authorizationState, manifest, catalog, claim);
      if (disposition != Disposition.AUTHORIZED) {
        // Specifics stay server-side; only the aggregate count is returned to the caller.
        LOGGER.info(
            "Omitting {} dataset {} in catalog {} from lineage event for principal {}",
            claim.role(),
            claim.table(),
            catalog,
            principal == null ? "unknown" : principal.getName());
      }
      dispositions.put(claim.dataset(), disposition);
    }
  }

  /**
   * Decides one claim: resolve first, and only ask for a privilege decision on something that
   * resolved.
   *
   * <p>The order matters. {@code AuthorizationIntentResolver} requires an intent's target to have
   * resolved and fails a precondition if it did not, so handing it an unresolved target would raise
   * a 500 rather than produce a deny. Probing the manifest first converts that into a clean {@code
   * OMITTED}.
   */
  private Disposition classify(
      AuthorizationState state, PolarisResolutionManifest manifest, String catalog, Claim claim) {
    if (claim.table() == null) {
      // Names a catalog of ours but no single table path: there is nothing to authorize against.
      return Disposition.OMITTED;
    }

    PolarisResolvedPathWrapper table = resolved(manifest, tablePathKey(claim.table()));
    if (table != null) {
      PolarisEntitySubType subType = table.getRawLeafEntity().getSubType();
      return decide(
          state, tableOperation(claim.role(), subType), tableSecurable(catalog, claim.table()));
    }

    if (claim.role() != Role.OUTPUT
        || resolved(manifest, namespacePathKey(claim.table())) == null) {
      // No namespace fallback for INPUT or STANDALONE -- see the class javadoc.
      return Disposition.OMITTED;
    }

    // Create-table-as-select: the output table does not exist yet, so the claim is authorized
    // against the namespace it would be created in.
    return decide(
        state,
        PolarisAuthorizableOperation.INGEST_TABLE_LINEAGE,
        namespaceSecurable(catalog, claim.table()));
  }

  /**
   * The resolved path for {@code key}, or {@code null} if it did not fully resolve for this caller.
   *
   * <p>All lineage operations register with the default {@code ResolvedPathRooting.ROOT}, so {@code
   * PolarisAuthorizerImpl} reads their paths back with {@code prependRootContainer = true}; this
   * probe matches, as every read-back in {@code CatalogHandler} does.
   */
  private static PolarisResolvedPathWrapper resolved(
      PolarisResolutionManifest manifest, ResolvedPathKey key) {
    return manifest.getResolvedPath(key, true);
  }

  private Disposition decide(
      AuthorizationState state, PolarisAuthorizableOperation operation, PolarisSecurable target) {
    AuthorizationRequest request =
        new AuthorizationRequest(
            principal, List.of(new SingleTargetAuthorizationIntent(operation, target)));
    AuthorizationDecision decision = authorizer.authorize(state, request);
    return decision.isAllowed() ? Disposition.AUTHORIZED : Disposition.OMITTED;
  }

  /**
   * Paths are registered <em>optional</em>, which is load-bearing: a non-optional path that fails
   * to resolve fails the whole {@code resolveAll()}, after which every lookup on the manifest
   * returns null and the intent trips a precondition -- turning a dataset that merely does not
   * exist into a 500 rather than a per-dataset drop. Each path and the key it is read back with are
   * derived from one {@link ResolvedPathKey}, so they cannot drift apart.
   */
  private static void register(
      PolarisResolutionManifest manifest, Set<ResolvedPathKey> registered, ResolvedPathKey key) {
    if (registered.add(key)) {
      manifest.addPath(new ResolverPath(key, true));
    }
  }

  private static ResolvedPathKey tablePathKey(TableIdentifier table) {
    return ResolvedPathKey.ofTableLike(table);
  }

  private static ResolvedPathKey namespacePathKey(TableIdentifier table) {
    return ResolvedPathKey.ofNamespace(table.namespace());
  }

  private static PolarisSecurable tableSecurable(String catalog, TableIdentifier table) {
    return PolarisSecurableMapper.tableLike(catalog, table);
  }

  private static PolarisSecurable namespaceSecurable(String catalog, TableIdentifier table) {
    return PolarisSecurableMapper.namespace(catalog, table.namespace());
  }

  /** Both the table and the view operation for {@code role}, for the resolution request. */
  private static List<PolarisAuthorizableOperation> tableOperations(Role role) {
    return List.of(
        tableOperation(role, PolarisEntitySubType.ICEBERG_TABLE),
        tableOperation(role, PolarisEntitySubType.ICEBERG_VIEW));
  }

  /**
   * Inputs are checked for read access to the entity; outputs and standalone datasets are checked
   * for ingest. Anything that is not a view is treated as a table.
   */
  private static PolarisAuthorizableOperation tableOperation(
      Role role, PolarisEntitySubType subType) {
    boolean view = subType == PolarisEntitySubType.ICEBERG_VIEW;
    if (role == Role.INPUT) {
      return view
          ? PolarisAuthorizableOperation.REFERENCE_LINEAGE_INPUT_VIEW
          : PolarisAuthorizableOperation.REFERENCE_LINEAGE_INPUT_TABLE;
    }
    return view
        ? PolarisAuthorizableOperation.INGEST_VIEW_LINEAGE
        : PolarisAuthorizableOperation.INGEST_TABLE_LINEAGE;
  }

  /**
   * One dataset occurrence that claimed a catalog, with the table it names, or {@code null} if it
   * names no single usable table.
   */
  private record Claim(LineageDatasetKey dataset, TableIdentifier table) {
    Role role() {
      return dataset.role();
    }
  }
}
