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

import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.user.User;

/**
 * A <code>GraphHook</code> can be registered with a {@link uk.gov.gchq.gaffer.graph.Graph} and will be
 * triggered before and after operation chains are executed on the graph.
 */
public interface GraphHook {
    /**
     * Called from {@link uk.gov.gchq.gaffer.graph.Graph} before an {@link OperationChain}
     * is executed.
     *
     * @param opChain the {@link OperationChain} being executed.
     * @param user    the {@link User} executing the operation chain
     */
    void preExecute(final OperationChain<?> opChain, final User user);

    /**
     * Called from {@link uk.gov.gchq.gaffer.graph.Graph} after an {@link OperationChain}
     * is executed.
     *
     * @param result  the result from the operation chain
     * @param opChain the {@link OperationChain} that was executed.
     * @param user    the {@link User} who executed the operation chain
     * @param <T>     the result type
     * @return result object
     */
    <T> T postExecute(final T result,
                      final OperationChain<?> opChain,
                      final User user);
}
