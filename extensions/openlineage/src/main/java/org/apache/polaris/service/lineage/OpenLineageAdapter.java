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
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.service.lineage.api.OpenLineageBatchIngestResponse;
import org.apache.polaris.service.lineage.api.PolarisLineageEvent;
import org.apache.polaris.service.lineage.api.PolarisOpenLineageApiService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adapter between the JAX-RS OpenLineage resource and the {@link OpenLineageIngestProvider}.
 *
 * <p>Responsible for authorizing the dataset references an event carries, translating the HTTP
 * request context into an {@link OpenLineageIngestRequest}, and mapping the provider result back to
 * a JAX-RS {@link Response}. Provider implementations do not interact with JAX-RS types.
 *
 * <p>Batch ingest is aggregated here rather than on the provider seam: the adapter fans a batch out
 * into per-event {@link OpenLineageIngestProvider#ingest} calls and assembles the per-event
 * outcomes into an {@link OpenLineageBatchIngestResponse}. This keeps the provider contract a
 * single event in / single result out.
 *
 * <h2>Authorization</h2>
 *
 * <p>Ingest authorization lives here and not in the provider. The provider is a pluggable SPI, and
 * RBAC is not something each implementation should have to re-derive — nor something a deployment
 * should be able to lose by swapping the provider out. The adapter hands the provider an event that
 * has already had every unauthorized dataset removed, so the provider cannot persist one even if it
 * ignores the contract entirely. See {@link LineageIngestAuthorizer}.
 *
 * <p>An event that authorizes nothing is a no-op that still <em>succeeds</em>. Lineage is
 * observability data arriving out-of-band from a job that has already run, so failing the caller
 * over a lineage-only authorization gap would break the engine's job for a reason unrelated to the
 * work it did; and because ingest is idempotent, replaying the event after the grants are fixed
 * backfills what was skipped. What the caller gets back is aggregate omitted counts grouped by
 * reason, never which dataset was omitted — that would be a membership oracle over the catalog
 * namespace.
 *
 * <p>When {@link FeatureConfiguration#ENABLE_OPENLINEAGE_INGEST} is disabled for the realm, both
 * endpoints return {@code 501 Not Implemented} without authorizing or invoking the provider. The
 * routes stay mounted whenever the extension is assembled, so this flag is the runtime switch that
 * turns ingest on or off (and it also gates whether the endpoints are advertised during discovery).
 *
 * <p>Request-scoped context ({@link RealmConfig}, {@link PolarisPrincipal}) is injected rather than
 * threaded through the {@link PolarisOpenLineageApiService} methods.
 */
@RequestScoped
public class OpenLineageAdapter implements PolarisOpenLineageApiService {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpenLineageAdapter.class);

  /**
   * Advisory response headers carrying the omitted counts for the single-event endpoint, which has
   * no response body to put them in. Emitted only when the corresponding count is non-zero.
   */
  static final String OMITTED_UNRESOLVED_HEADER = "Polaris-Lineage-Omitted-Unresolved";

  static final String OMITTED_UNAUTHORIZED_HEADER = "Polaris-Lineage-Omitted-Unauthorized";

  private final OpenLineageIngestProvider provider;
  private final RealmConfig realmConfig;
  private final LineageIngestAuthorizer lineageAuthorizer;

  @Inject
  public OpenLineageAdapter(
      OpenLineageIngestProvider provider,
      RealmConfig realmConfig,
      PolarisPrincipal principal,
      ResolutionManifestFactory resolutionManifestFactory,
      PolarisAuthorizer authorizer) {
    this(
        provider,
        realmConfig,
        new LineageIngestAuthorizer(
            principal,
            resolutionManifestFactory,
            authorizer,
            PolarisDatasetIdentifier.fromRealmConfig(realmConfig)));
  }

  OpenLineageAdapter(
      OpenLineageIngestProvider provider,
      RealmConfig realmConfig,
      LineageIngestAuthorizer lineageAuthorizer) {
    this.provider = provider;
    this.realmConfig = realmConfig;
    this.lineageAuthorizer = lineageAuthorizer;
  }

  @Override
  public Response sendLineageEvent(PolarisLineageEvent event) {
    if (!openLineageEnabled()) {
      return Response.status(Response.Status.NOT_IMPLEMENTED).build();
    }
    return toResponse(ingestOne(event));
  }

  @Override
  public Response sendLineageEventBatch(List<PolarisLineageEvent> events) {
    if (!openLineageEnabled()) {
      return Response.status(Response.Status.NOT_IMPLEMENTED).build();
    }
    int successful = 0;
    int omittedUnresolved = 0;
    int omittedUnauthorized = 0;
    List<OpenLineageBatchIngestResponse.FailedEvent> failed = new ArrayList<>();

    for (int i = 0; i < events.size(); i++) {
      Outcome outcome;
      try {
        outcome = ingestOne(events.get(i));
      } catch (RuntimeException e) {
        // One bad event must not abort the batch. The ingest provider is a pluggable SPI that can
        // throw anything, and the Iceberg exception mapper is registered globally across
        // /api/openlineage/v1, so an escaping RuntimeException would otherwise be mapped into a
        // single error response for the whole batch and lose the outcomes of every other event.
        // Reported as non-retriable because the provider already models the transient case as
        // UNAVAILABLE, so an unexpected throw is more likely deterministic; calling it retriable
        // would invite a client to replay the batch forever.
        LOGGER.warn("Lineage event at batch index {} failed unexpectedly", i, e);
        failed.add(
            new OpenLineageBatchIngestResponse.FailedEvent(
                i, false, "Event could not be processed"));
        continue;
      }
      // Omissions are reported for every event that was not itself a failure, including the no-op
      // case: an event that authorized nothing is a success with everything omitted.
      omittedUnresolved += outcome.authorization().omittedUnresolvedCount();
      omittedUnauthorized += outcome.authorization().omittedUnauthorizedCount();
      switch (outcome.result()) {
        case ACCEPTED -> successful++;
        case REJECTED ->
            failed.add(new OpenLineageBatchIngestResponse.FailedEvent(i, false, "Event rejected"));
        case UNAVAILABLE ->
            failed.add(
                new OpenLineageBatchIngestResponse.FailedEvent(
                    i, true, "Ingest backend unavailable"));
      }
    }

    // Omitted datasets are not failures, so they do not influence the batch status: the existing
    // SUCCESS/PARTIAL/FAILURE semantics continue to describe provider outcomes only.
    OpenLineageBatchIngestResponse.Status status;
    if (failed.isEmpty()) {
      status = OpenLineageBatchIngestResponse.Status.SUCCESS;
    } else if (successful == 0) {
      status = OpenLineageBatchIngestResponse.Status.FAILURE;
    } else {
      status = OpenLineageBatchIngestResponse.Status.PARTIAL;
    }

    OpenLineageBatchIngestResponse body =
        new OpenLineageBatchIngestResponse(
            status,
            new OpenLineageBatchIngestResponse.Summary(
                events.size(), successful, failed.size(), omittedUnresolved, omittedUnauthorized),
            failed);
    return Response.status(Response.Status.OK).entity(body).build();
  }

  private boolean openLineageEnabled() {
    return realmConfig.getConfig(FeatureConfiguration.ENABLE_OPENLINEAGE_INGEST);
  }

  private Outcome ingestOne(PolarisLineageEvent event) {
    OpenLineage.BaseEvent rawEvent = event == null ? null : event.event();
    LineageAuthorizationResult authorization = lineageAuthorizer.authorize(rawEvent);

    if (authorization.isNoOp()) {
      // Nothing in this event is the caller's to assert, so there is nothing to record. Accepted
      // rather than refused: see the class javadoc.
      return new Outcome(OpenLineageIngestResult.ACCEPTED, authorization);
    }

    OpenLineage.BaseEvent authorizedEvent =
        LineageEventFilter.retainAuthorized(rawEvent, authorization);
    return new Outcome(
        provider.ingest(new OpenLineageIngestRequest(authorizedEvent)), authorization);
  }

  private static Response toResponse(Outcome outcome) {
    Response.ResponseBuilder builder =
        switch (outcome.result()) {
          case ACCEPTED -> Response.status(Response.Status.CREATED);
          case REJECTED -> Response.status(Response.Status.BAD_REQUEST);
          case UNAVAILABLE -> Response.status(Response.Status.SERVICE_UNAVAILABLE);
        };
    LineageAuthorizationResult authorization = outcome.authorization();
    if (authorization.omittedUnresolvedCount() > 0) {
      builder.header(OMITTED_UNRESOLVED_HEADER, authorization.omittedUnresolvedCount());
    }
    if (authorization.omittedUnauthorizedCount() > 0) {
      builder.header(OMITTED_UNAUTHORIZED_HEADER, authorization.omittedUnauthorizedCount());
    }
    return builder.build();
  }

  /** The provider result for one event, paired with what authorization decided about it. */
  private record Outcome(
      OpenLineageIngestResult result, LineageAuthorizationResult authorization) {}
}
