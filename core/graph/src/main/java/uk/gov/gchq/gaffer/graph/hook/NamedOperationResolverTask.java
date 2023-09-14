/*
 * Copyright 2023 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package uk.gov.gchq.gaffer.graph.hook;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.operation.Operations;
import uk.gov.gchq.gaffer.store.operation.handler.named.cache.NamedOperationCache;
import uk.gov.gchq.gaffer.user.User;

public class NamedOperationResolverTask implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(NamedOperationResolverTask.class);

    private final Operations<?> operations;
    private final User user;
    private final NamedOperationCache cache;

    public NamedOperationResolverTask(final Operations<?> operations, final User user, final NamedOperationCache cache) {
        this.operations = operations;
        this.user = user;
        this.cache = cache;
    }

    @Override
    public void run() {
        NamedOperationResolver.resolveNamedOperations(operations, user, cache);
        if (Thread.interrupted()) {
            LOGGER.error("resolving named operations is interrupted, exiting");
        }
    }
}
