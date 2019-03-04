/*
 * Copyright 2019 Crown Copyright
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
package uk.gov.gchq.gaffer.store.util;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.store.Context;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
public interface Hook {
    /**
     * @param opChain the {@link OperationChain} being executed. This can be modified/optimised in any GraphHook.
     * @param context the {@link Context} in which the operation chain was executed. The context also holds a reference to the original operation chain.
     * @deprecated use Operation method not OperationChain
     * Called from {@link uk.gov.gchq.gaffer.store.Store} before an {@link OperationChain}
     * is executed.
     */
    default void preExecute(final OperationChain<?> opChain, final Context context) {
        preExecute(new Request(opChain, context));
    }

    default void preExecute(final Request request) {
    }

    /**
     * @param result  the result from the operation chain
     * @param opChain the {@link OperationChain} that was executed. This can be modified/optimised in any GraphHook.
     * @param context the {@link Context} in which the operation chain was executed. The context also holds a reference to the original operation chain.
     * @param <T>     the result type
     * @return result object
     * @deprecated use Operation method not OperationChain
     * Called from {@link uk.gov.gchq.gaffer.store.Store} after an {@link OperationChain}
     * is executed.
     */
    default <T> T postExecute(final T result,
                              final OperationChain<?> opChain,
                              final Context context) {
        return postExecute(result, new Request(opChain, context));
    }

    default <T> T postExecute(final T result,
                              final Request request) {
        return result;
    }

    /**
     * @param <T>     the result type
     * @param result  the result from the operation chain - likely to be null.
     * @param opChain the {@link OperationChain} that was executed. This can be modified/optimised in any GraphHook.
     * @param context the {@link Context} in which the operation chain was executed. The context also holds a reference to the original operation chain.
     * @param e       the exception
     * @return result object
     * @deprecated use Operation method not OperationChain
     * Called from {@link uk.gov.gchq.gaffer.store.Store} if an error occurs whilst
     * executing the {@link OperationChain}.
     */
    default <T> T onFailure(final T result,
                            final OperationChain<?> opChain,
                            final Context context,
                            final Exception e) {
        return onFailure(result, new Request(opChain, context), e);
    }

    default <T> T onFailure(final T result,
                            final Request request,
                            final Exception e) {
        return result;
    }
}
