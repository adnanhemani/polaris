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

package org.apache.polaris.jpa.models;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.util.Optional;

@Entity
@Table(name = "EVENTS")
public class ModelEvent {
    // event id
    @Id private String eventId;

    // id of the request that generated this event
    private String requestId;

    // amount of events duplicate events that were generated
    private long eventCount;

    // timestamp in epoch milliseconds of when this event was emitted
    private long timestampMs;

    // An ordered list of actors involved in the operation, with the most direct actor
    // (the one who actually performed the operation) first, followed by delegating actors
    // in order. For example, if a service account (actor[0]) performed an operation
    // on behalf of a role (actor[1]) assumed by a user (actor[2]), the chain represents
    // this delegation path.
    // actor object, serialized as a JSON string
    private String actor;

    // Operation object that have their own required fields
    @Column(columnDefinition = "TEXT")
    private String operation;

    public static final String EMPTY_MAP_STRING = "{}";

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

    public String getOperation() {
        return operation != null ? operation : EMPTY_MAP_STRING;
    }
}
