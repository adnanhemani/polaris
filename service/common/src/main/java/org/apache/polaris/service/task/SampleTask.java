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

package org.apache.polaris.service.task;

import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.AsyncTaskType;
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;

public class SampleTask implements TaskHandler {
    MetaStoreManagerFactory metaStoreManagerFactory;

    public SampleTask(MetaStoreManagerFactory metaStoreManagerFactory) {
        this.metaStoreManagerFactory = metaStoreManagerFactory;
    }

    @Override
    public boolean canHandleTask(TaskEntity task) {
        return task.getTaskType() == AsyncTaskType.SAMPLE_TASK;
    }

    @Override
    public boolean handleTask(TaskEntity task, CallContext callContext) {
        String realmId = callContext.getRealmContext().getRealmIdentifier();
        var unused = this.metaStoreManagerFactory.getOrCreateSessionSupplier(() -> realmId).get();
        return true;
    }
}
