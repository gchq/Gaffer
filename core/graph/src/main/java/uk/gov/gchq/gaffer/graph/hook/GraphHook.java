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
 * <p>
 * A {@code GraphHook} can be registered with a {@link uk.gov.gchq.gaffer.graph.Graph} and will be
 * triggered before and after operation chains are executed on the graph.
 * </p>
 * <p>
 * If an error occurs whilst running the operation chain the onFailure method will be
 * triggered.
 * </p>
 * <p>
 * The {@link OperationChain} parameter that is given to the graph hook is
 * a clone of the original and can be adapted or optimised in any GraphHook
 * implementation. So please note that if you want to access the original
 * operation chain without modifications then you can access if from
 * {@link Context#getOriginalOpChain()}. This original operation chain should not
 * be modified.
 * </p>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
public interface GraphHook {
    /**
     * Called from {@link uk.gov.gchq.gaffer.graph.Graph} before an {@link OperationChain}
     * is executed.
     *
     * @param opChain the {@link OperationChain} being executed. This can be modified/optimised in any GraphHook.
     * @param context the {@link Context} in which the operation chain was executed. The context also holds a reference to the original operation chain.
     */
    void preExecute(final OperationChain<?> opChain, final Context context);

    /**
     * Called from {@link uk.gov.gchq.gaffer.graph.Graph} after an {@link OperationChain}
     * is executed.
     * NOTE - if you do not wish to use this method you must still implement it and just return the result unmodified.
     *
     * @param result  the result from the operation chain
     * @param opChain the {@link OperationChain} that was executed. This can be modified/optimised in any GraphHook.
     * @param context the {@link Context} in which the operation chain was executed. The context also holds a reference to the original operation chain.
     * @param <T>     the result type
     * @return result object
     */
    <T> T postExecute(final T result,
                      final OperationChain<?> opChain,
                      final Context context);

    /**
     * Called from {@link uk.gov.gchq.gaffer.graph.Graph} if an error occurs whilst
     * executing the {@link OperationChain}.
     * NOTE - if you do not wish to use this method you must still implement it and just return the result unmodified.
     *
     * @param <T>     the result type
     * @param result  the result from the operation chain - likely to be null.
     * @param opChain the {@link OperationChain} that was executed. This can be modified/optimised in any GraphHook.
     * @param context the {@link Context} in which the operation chain was executed. The context also holds a reference to the original operation chain.
     * @param e       the exception
     * @return result object
     */
    <T> T onFailure(final T result,
                    final OperationChain<?> opChain,
                    final Context context,
                    final Exception e);
}
