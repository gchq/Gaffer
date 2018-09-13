/*
 * Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.handler;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.Join;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.operation.util.join.JoinFunction;
import uk.gov.gchq.gaffer.operation.util.join.JoinType;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import java.util.ArrayList;

public class JoinHandler<I, O> implements OutputOperationHandler<Join<I, O>, Iterable<? extends O>> {
    @Override
    public Iterable<? extends O> doOperation(final Join<I, O> operation, final Context context, final Store store) throws OperationException {
        JoinFunction joinFunction = operation.getJoinType().createInstance();

        if (null == operation.getInput()) {
            operation.setInput(new ArrayList<>());
        }

        if (null != operation.getMatchingOnIterable() && !operation.getJoinType().equals(JoinType.INNER)) {
            throw new OperationException("It is only possible to specify an Iterable to match on when using Inner join type");
        }

        if (null == operation.getMatchingOnIterable() && operation.getJoinType().equals(JoinType.INNER)) {
            throw new OperationException("You must specify an Iterable to match on when using Inner join type");
        }

        Iterable<I> rightIterable = new ArrayList<>();
        if (operation.getOperation() instanceof Output) {
            rightIterable = (Iterable<I>) store.execute((Output) operation.getOperation(), context);
        }

        Iterable joinResults = joinFunction.join(operation.getInput(), rightIterable, operation.getMatcher(), operation.getMatchingOnIterable());

        return operation.getReducer().reduce(joinResults);
    }
}
