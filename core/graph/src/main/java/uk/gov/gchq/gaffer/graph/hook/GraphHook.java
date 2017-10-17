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

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.store.Context;

/**
 * A {@code GraphHook} can be registered with a {@link uk.gov.gchq.gaffer.graph.Graph} and will be
 * triggered before and after operation chains are executed on the graph. If an
 * error occurs whilst running the operation chain the onFailure method will be
 * triggered.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
public interface GraphHook {
    /**
     * Called from {@link uk.gov.gchq.gaffer.graph.Graph} before an {@link OperationChain}
     * is executed.
     *
     * @param opChain the {@link OperationChain} being executed.
     * @param context the {@link Context} in which the operation chain was executed
     */
    void preExecute(final OperationChain<?> opChain, final Context context);

    /**
     * Called from {@link uk.gov.gchq.gaffer.graph.Graph} after an {@link OperationChain}
     * is executed.
     *
     * @param result  the result from the operation chain
     * @param opChain the {@link OperationChain} that was executed.
     * @param context the {@link Context} in which the operation chain was executed
     * @param <T>     the result type
     * @return result object
     */
    <T> T postExecute(final T result,
                      final OperationChain<?> opChain,
                      final Context context);

    /**
     * Called from {@link uk.gov.gchq.gaffer.graph.Graph} if an error occurs whilst
     * executing the {@link OperationChain}.
     *
     * @param <T>     the result type
     * @param result  the result from the operation chain - likely to be null.
     * @param opChain the {@link OperationChain} that was executed.
     * @param context the {@link Context} in which the operation chain was executed
     * @param e       the exception
     * @return result object
     */
    <T> T onFailure(final T result,
                    final OperationChain<?> opChain,
                    final Context context,
                    final Exception e);
}
