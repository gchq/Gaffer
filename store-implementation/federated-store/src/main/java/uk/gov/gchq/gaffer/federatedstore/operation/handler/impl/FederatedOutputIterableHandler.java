/*
 * Copyright 2017-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.operation.handler.impl;

import uk.gov.gchq.gaffer.commonutil.iterable.EmptyIterable;
import uk.gov.gchq.gaffer.operation.io.Output;

/**
 * Handler for the federation of an PAYLOAD operation with an expected return type Iterable
 *
 * @param <PAYLOAD>           The operation to be federated and executed by delegate graphs.
 * @param <ITERABLE_ELEMENTS> the type of elements returned by the Output Iterable
 * @see uk.gov.gchq.gaffer.store.operation.handler.OperationHandler
 * @see uk.gov.gchq.gaffer.federatedstore.FederatedStore
 * @see uk.gov.gchq.gaffer.operation.impl.get.GetElements
 */
public class FederatedOutputIterableHandler<PAYLOAD extends Output< Iterable<? extends ITERABLE_ELEMENTS>>, ITERABLE_ELEMENTS> extends FederatedOutputHandler<PAYLOAD, Iterable<? extends ITERABLE_ELEMENTS>> {
    public FederatedOutputIterableHandler() {
        super(new EmptyIterable<>());
    }
}
