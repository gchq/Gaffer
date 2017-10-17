/*
 * Copyright 2016 Crown Copyright
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

import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.store.Context;

/**
 * A {@code Log4jLogger} is a simple {@link GraphHook} that sends logs of the
 * operation chains executed by users on a graph to a {@link Logger}.
 */
public class Log4jLogger implements GraphHook {
    private static final Logger LOGGER = LoggerFactory.getLogger(Log4jLogger.class);

    /**
     * Logs the operation chain and the user id.
     *
     * @param opChain the operation chain being executed
     * @param context    the Context executing the operation chain
     */
    @Override
    public void preExecute(final OperationChain<?> opChain, final Context context) {
        LOGGER.info("Running {} as {}", opChain, context.getUser().getUserId());
    }

    @Override
    public <T> T postExecute(final T result, final OperationChain<?> operationChain, final Context context) {
        // No logging required.
        return result;
    }

    @Override
    public <T> T onFailure(final T result, final OperationChain<?> opChain, final Context context, final Exception e) {
        LOGGER.warn("Failed to run {} as {}", opChain, context.getUser().getUserId());
        return result;
    }
}
