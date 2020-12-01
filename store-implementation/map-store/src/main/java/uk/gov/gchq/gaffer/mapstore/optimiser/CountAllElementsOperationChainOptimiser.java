/*
 * Copyright 2020 Crown Copyright
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

package uk.gov.gchq.gaffer.mapstore.optimiser;

import uk.gov.gchq.gaffer.mapstore.operation.CountAllElementsDefaultView;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.impl.Count;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.optimiser.AbstractOperationChainOptimiser;

import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static uk.gov.gchq.gaffer.data.element.id.DirectedType.EITHER;

public class CountAllElementsOperationChainOptimiser extends AbstractOperationChainOptimiser {

    @Override
    protected List<Operation> addPreOperations(final Operation previousOp, final Operation currentOp) {
        return emptyList();
    }

    @Override
    protected List<Operation> optimiseCurrentOperation(final Operation previousOp, final Operation currentOp, final Operation nextOp) {
        if (isNonFilteringGetAllElementsOperation(previousOp) && isCountOperation(currentOp)) {
            return singletonList(new CountAllElementsDefaultView.Builder().build());
        }
        if (isNonFilteringGetAllElementsOperation(currentOp) && isCountOperation(nextOp)) {
            return emptyList();
        }
        return singletonList(currentOp);
    }

    @Override
    protected List<Operation> addPostOperations(final Operation currentOp, final Operation nextOp) {
        return emptyList();
    }

    @Override
    protected List<Operation> optimiseAll(final List<Operation> ops) {
        return ops;
    }

    private boolean isNonFilteringGetAllElementsOperation(final Operation operation) {
        if (null != operation && GetAllElements.class.equals(operation.getClass())) {
            final GetAllElements getAllElements = GetAllElements.class.cast(operation);
            return (null == getAllElements.getView()
                    && (null == getAllElements.getDirectedType() || getAllElements.getDirectedType().equals(EITHER)));
        }
        return false;
    }

    private boolean isCountOperation(final Operation operation) {
        return null != operation && Count.class.equals(operation.getClass());
    }
}
