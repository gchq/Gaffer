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

package uk.gov.gchq.gaffer.store.optimiser;

import uk.gov.gchq.gaffer.operation.OperationChain;

/**
 * Optimises and applies preprocessing to operation chains.
 */
public interface OperationChainOptimiser {
    /**
     * Optimises the operation chain. Operations in the chain can be swapped for
     * more efficient operations depending on the store implementation.
     * The operation chain has already been cloned so changes may be made directly to the operation chain
     * parameter if required.
     * This method can be extended to add custom optimisation, but ensure super.optimise is called first.
     * Alternatively, the preferred approach is to override addPreOperations, optimiseCurrentOperation or addPostOperations
     *
     * @param operationChain the operation chain to optimise
     * @param <OUTPUT>       the operation output type
     * @return the optimised operation chain
     */
    <OUTPUT> OperationChain<OUTPUT> optimise(final OperationChain<OUTPUT> operationChain);
}
