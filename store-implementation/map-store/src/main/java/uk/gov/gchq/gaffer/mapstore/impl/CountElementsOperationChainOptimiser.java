/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.mapstore.impl;

import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.mapstore.operation.CountAllElementsDefaultView;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.impl.Count;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.optimiser.AbstractOperationChainOptimiser;

import java.util.Collections;
import java.util.List;

/**
 *
 */
public class CountElementsOperationChainOptimiser extends AbstractOperationChainOptimiser {

    @Override
    protected List<Operation> addPreOperations(final Operation<?, ?> previousOp, final Operation<?, ?> currentOp) {
        return Collections.emptyList();
    }

    @Override
    protected List<Operation> optimiseCurrentOperation(final Operation<?, ?> previousOp,
                                                       final Operation<?, ?> currentOp,
                                                       final Operation<?, ?> nextOp) {
        return Collections.singletonList(currentOp);
    }

    @Override
    protected List<Operation> addPostOperations(final Operation<?, ?> currentOp, final Operation<?, ?> nextOp) {
        return Collections.emptyList();
    }

    /**
     * If the list of operations consists of a <code>GetAllElements</code> with the default {@link View} followed by a
     * {@link Count} operation then this can be optimised to a single {@link CountAllElementsDefaultView} operation.
     * Otherwise the original list of operations is returned.
     *
     * @param ops the operations to be optimised.
     * @return the optimised list of operations.
     */
    @Override
    protected List<Operation> optimiseAll(final List<Operation> ops) {
        if (2 != ops.size()) {
            return ops;
        }
        final Operation op1 = ops.get(0);
        final Operation op2 = ops.get(1);
        if (!(op1 instanceof GetAllElements) || !(op2 instanceof Count)) {
            return ops;
        }
        final GetAllElements<?> getAllElements = (GetAllElements<?>) op1;
        final View view = getAllElements.getView();
        if (null != view && !view.equals(new View.Builder().build())) {
            return ops;
        }
        return Collections.singletonList(new CountAllElementsDefaultView());
    }
}
