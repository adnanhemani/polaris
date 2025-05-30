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
package org.apache.polaris.service.events.listeners;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisEvent;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;

import java.util.ArrayList;
import java.util.List;

/** Event listener that buffers in memory and then dumps to persistence. */
@ApplicationScoped
@Identifier("persistence-in-memory-buffer")
public class InMemoryBufferPolarisEventListener extends PersistencePolarisEventListener {
    MetaStoreManagerFactory metaStoreManagerFactory;
    PolarisConfigurationStore polarisConfigurationStore;

    private final List<PolarisEvent> buffer = new ArrayList<>();
    private final int timeToFlush;

    @Inject
    public InMemoryBufferPolarisEventListener(
            MetaStoreManagerFactory metaStoreManagerFactory,
            PolarisConfigurationStore polarisConfigurationStore
    ) {
        this.metaStoreManagerFactory = metaStoreManagerFactory;
        this.polarisConfigurationStore = polarisConfigurationStore;
        this.timeToFlush = polarisConfigurationStore.getConfiguration(null, FeatureConfiguration.EVENT_BUFFER_TIME_TO_FLUSH);
    }

    @Override
    void addToBuffer(PolarisEvent polarisEvent, CallContext callCtx) {
        buffer.add(polarisEvent);
        if (System.currentTimeMillis() - buffer.getFirst().getTimestampMs() > this.timeToFlush) {
            metaStoreManagerFactory.getOrCreateSessionSupplier(callCtx.getRealmContext()).get().writeEvents(buffer);
            buffer.clear();
        }
    }
}
