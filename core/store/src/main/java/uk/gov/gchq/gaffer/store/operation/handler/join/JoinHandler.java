/*
 * Copyright 2018-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.handler.join;


import uk.gov.gchq.gaffer.commonutil.exception.LimitExceededException;
import uk.gov.gchq.gaffer.commonutil.iterable.LimitedCloseableIterable;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.join.Join;
import uk.gov.gchq.gaffer.operation.impl.join.match.MatchKey;
import uk.gov.gchq.gaffer.operation.impl.join.methods.JoinFunction;
import uk.gov.gchq.gaffer.operation.impl.join.methods.JoinType;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.koryphe.tuple.MapTuple;

import java.util.ArrayList;

import static uk.gov.gchq.gaffer.store.operation.handler.util.OperationHandlerUtil.getResultsOrNull;
import static uk.gov.gchq.gaffer.store.operation.handler.util.OperationHandlerUtil.updateOperationInput;

public class JoinHandler<I> implements OutputOperationHandler<Join<I>, Iterable<? extends MapTuple>> {
    @Override
    public Iterable<? extends MapTuple> doOperation(final Join<I> operation, final Context context, final Store store) throws OperationException {
        final int limit = operation.getCollectionLimit() != null ? operation.getCollectionLimit() : 100000;

        if (null == operation.getJoinType()) {
            throw new OperationException("A join type must be specified");
        }

        if (null == operation.getMatchMethod()) {
            throw new OperationException("A match method must be supplied");
        }

        if (null == operation.getInput()) {
            operation.setInput(new ArrayList<>());
        }

        MatchKey matchKey = operation.getMatchKey();

        if (null == matchKey) {
            if (!operation.getJoinType().equals(JoinType.INNER)) {
                throw new OperationException("You must specify an Iterable side to match on");
            }
            // setting match key to avoid swapping inputs
            matchKey = MatchKey.LEFT;
        }


        JoinFunction joinFunction = operation.getJoinType().createInstance();

        updateOperationInput(operation.getOperation(), null);
        Iterable<I> rightIterable =
                (Iterable<I>) getResultsOrNull(operation.getOperation(),
                        context,
                        store);

        final Iterable limitedLeftIterable;
        final Iterable limitedRightIterable;

        try {
            limitedLeftIterable = new LimitedCloseableIterable(operation.getInput(), 0, limit, false);
            limitedRightIterable = new LimitedCloseableIterable(rightIterable, 0, limit, false);
            return joinFunction.join(limitedLeftIterable, limitedRightIterable, operation.getMatchMethod(), matchKey, operation.isFlatten());
        } catch (final LimitExceededException e) {
            throw new OperationException("Join exceeded the collectionLimit, a solution is to increasing collectionLimit value in the join operation.", e);
        }

    }
}
