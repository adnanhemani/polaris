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
package org.apache.polaris.service.events;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEvent;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Event listener that buffers in memory and then dumps to persistence. */
@ApplicationScoped
@Identifier("persistence-in-memory-buffer")
public class InMemoryBufferPolarisEventListener extends PolarisEventListener {
    @Inject
    MetaStoreManagerFactory metaStoreManagerFactory;

    private final List<PolarisEvent> buffer = new ArrayList<>();
    private int timeToFlush = -1;

    @Override
    public void onBeforeRequestRateLimited(BeforeRequestRateLimitedEvent event) {
    }

    @Override
    public void onBeforeTableCommited(BeforeTableCommitedEvent event) {
    }

    @Override
    public void onAfterTableCommited(AfterTableCommitedEvent event) {
    }

    @Override
    public void onBeforeViewCommited(BeforeViewCommitedEvent event) {
    }

    @Override
    public void onAfterViewCommited(AfterViewCommitedEvent event) {
    }

    @Override
    public void onBeforeTableRefreshed(BeforeTableRefreshedEvent event) {
    }

    @Override
    public void onAfterTableRefreshed(AfterTableRefreshedEvent event) {
    }

    @Override
    public void onBeforeViewRefreshed(BeforeViewRefreshedEvent event) {
    }

    @Override
    public void onAfterViewRefreshed(AfterViewRefreshedEvent event) {
    }

    @Override
    public void onBeforeTaskAttempted(BeforeTaskAttemptedEvent event) {
    }

    @Override
    public void onAfterTaskAttempted(AfterTaskAttemptedEvent event) {
    }

    @Override
    public void onBeforeTableCreated(BeforeTableCreatedEvent event) {
    }

    @Override
    public void onAfterTableCreated(AfterTableCreatedEvent event, RealmContext realmContext) {
        PolarisEvent polarisEvent = new PolarisEvent(
                event.getEventId(),
                event.getRequestId(),
                event.getEventCount(),
                event.getTimestampMs(),
                event.getActor(),
                event.getIcebergOperationType(),
                event.getPolarisCustomOperationType(),
                event.getResourceType(),
                event.getTableIdentifier().toString());
        addToBuffer(polarisEvent, realmContext);
    }

    @Override
    public void onAfterCatalogCreated(AfterCatalogCreatedEvent event, RealmContext realmContext) {
        PolarisEvent polarisEvent = new PolarisEvent(
                event.getEventId(),
                event.getRequestId(),
                event.getEventCount(),
                event.getTimestampMs(),
                event.getActor(),
                event.getIcebergOperationType(),
                event.getPolarisCustomOperationType(),
                event.getResourceType(),
                event.getCatalogName());
        addToBuffer(polarisEvent, realmContext);
    }

    private void addToBuffer(PolarisEvent polarisEvent, RealmContext realmContext) {
        buffer.add(polarisEvent);
        if (System.currentTimeMillis() - buffer.getFirst().getTimestampMs() > timeToFlush) {
            metaStoreManagerFactory.getOrCreateSessionSupplier(realmContext).get().writeEvents(buffer);
            buffer.clear();
        }
    }
}
