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

package org.apache.polaris.extension.persistence.relational.jdbc.models;

import org.apache.polaris.core.entity.PolarisEvent;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class ModelEvent implements Converter<PolarisEvent> {
    // event id
    private String eventId;

    // id of the request that generated this event
    private String requestId;

    // amount of events duplicate events that were generated
    private long eventCount;

    // timestamp in epoch milliseconds of when this event was emitted
    private long timestampMs;

    // polaris principal who took this action
    private String actor;

    // Operation type, as defined by the Iceberg Events API spec
    private String icebergOperationType;

    // Optional field that represents the Polaris custom events that are not modeled by the Iceberg Events API spec
    private String polarisCustomOperationType;

    // Enum that states the type of resource was being operated on
    private PolarisEvent.ResourceType resourceType;

    // Which resource was operated on
    private String resourceIdentifier;

    // Additional parameters that were not earlier recorded
    private String additionalParameters;

    public String getEventId() {
        return eventId;
    }

    public String getRequestId() {
        return requestId;
    }

    public long getEventCount() {
        return eventCount;
    }

    public long getTimestampMs() {
        return timestampMs;
    }

    public String getActor() {
        return actor;
    }

    public String getIcebergOperationType() {
        return icebergOperationType;
    }

    public String getPolarisCustomOperationType() {
        return polarisCustomOperationType;
    }

    public PolarisEvent.ResourceType getResourceType() {
        return resourceType;
    }

    public String getResourceIdentifier() {
        return resourceIdentifier;
    }

    public String getAdditionalParameters() {
        return additionalParameters;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public PolarisEvent fromResultSet(ResultSet rs) throws SQLException {
        var modelEvent =
                ModelEvent.builder()
                        .eventId(rs.getString("event_id"))
                        .requestId(rs.getString("request_id"))
                        .eventCount(rs.getLong("event_count"))
                        .timestampMs(rs.getLong("timestamp_ms"))
                        .actor(rs.getString("actor"))
                        .icebergOperationType(rs.getString("iceberg_operation_type"))
                        .polarisOperationType(rs.getString("polaris_custom_operation_type"))
                        .resourceType(PolarisEvent.ResourceType.valueOf(rs.getString("resource_type")))
                        .resourceIdentifier(rs.getString("resource_identifier"))
                        .additionalParameters(rs.getString("additional_parameters"))
                        .build();
        return toEvent(modelEvent);
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("event_id", this.eventId);
        map.put("request_id", this.requestId);
        map.put("event_count", this.eventCount);
        map.put("timestamp_ms", this.timestampMs);
        map.put("actor", this.actor);
        map.put("iceberg_operation_type", this.icebergOperationType);
        map.put("polaris_custom_operation_type", Objects.requireNonNullElse(polarisCustomOperationType, ""));
        map.put("resource_type", this.resourceType);
        map.put("resource_identifier", this.resourceIdentifier);
        map.put("additional_parameters", this.additionalParameters);
        return map;
    }

    public static final class Builder {
        private final ModelEvent event;

        private Builder() {
            event = new ModelEvent();
        }

        public ModelEvent.Builder eventId(String id) {
            event.eventId = id;
            return this;
        }

        public ModelEvent.Builder requestId(String requestId) {
            event.requestId = requestId;
            return this;
        }

        public ModelEvent.Builder eventCount(long eventCount) {
            event.eventCount = eventCount;
            return this;
        }

        public ModelEvent.Builder timestampMs(long timestampMs) {
            event.timestampMs = timestampMs;
            return this;
        }

        public ModelEvent.Builder actor(String actorChain) {
            event.actor = actorChain;
            return this;
        }

        public ModelEvent.Builder icebergOperationType(String icebergOperationType) {
            event.icebergOperationType = icebergOperationType;
            return this;
        }

        public ModelEvent.Builder polarisOperationType(String polarisCustomperationType) {
            event.polarisCustomOperationType = polarisCustomperationType;
            return this;
        }

        public ModelEvent.Builder resourceType(PolarisEvent.ResourceType resourceType) {
            event.resourceType = resourceType;
            return this;
        }

        public ModelEvent.Builder resourceIdentifier(String resourceIdentifier) {
            event.resourceIdentifier = resourceIdentifier;
            return this;
        }

        public ModelEvent.Builder additionalParameters(String additionalParameters) {
            event.additionalParameters = additionalParameters;
            return this;
        }

        public ModelEvent build() {
            return event;
        }
    }

    public static ModelEvent fromEvent(PolarisEvent event) {
        if (event == null) return null;

        ModelEvent.Builder modelEventBuilder = ModelEvent.builder()
                .eventId(event.getId())
                .requestId(event.getRequestId())
                .eventCount(event.getEventCount())
                .timestampMs(event.getTimestampMs())
                .actor(event.getActor())
                .icebergOperationType(event.getIcebergOperationType())
                .polarisOperationType(event.getPolarisCustomOperationType())
                .resourceType(event.getResourceType())
                .resourceIdentifier(event.getResourceIdentifier())
                .additionalParameters(event.getAdditionalParameters());
        return modelEventBuilder.build();
    }

    public static PolarisEvent toEvent(ModelEvent model) {
        if (model == null) return null;

        PolarisEvent polarisEvent = new PolarisEvent(
                model.getEventId(),
                model.getRequestId(),
                model.getEventCount(),
                model.getTimestampMs(),
                model.getActor(),
                model.getIcebergOperationType(),
                Optional.ofNullable(model.getPolarisCustomOperationType()),
                model.getResourceType(),
                model.getResourceIdentifier()
        );
        polarisEvent.setAdditionalParameters(model.getAdditionalParameters());
        return polarisEvent;
    }
}
