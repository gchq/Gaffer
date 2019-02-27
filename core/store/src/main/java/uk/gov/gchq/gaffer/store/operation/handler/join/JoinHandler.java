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

import com.google.common.collect.Lists;

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
import java.util.List;

import static uk.gov.gchq.gaffer.store.operation.handler.util.OperationHandlerUtil.getResultsOrNull;
import static uk.gov.gchq.gaffer.store.operation.handler.util.OperationHandlerUtil.updateOperationInput;

public class JoinHandler<I, O> implements OutputOperationHandler<Join<I, O>, Iterable<? extends MapTuple>> {
    @Override
    public Iterable<? extends MapTuple> doOperation(final Join<I, O> operation, final Context context, final Store store) throws OperationException {
        final int limit = operation.getCollectionLimit() != null ? operation.getCollectionLimit() : 100000;

        if (null == operation.getJoinType()) {
            throw new OperationException("A join type must be specified");
        }

        if (null == operation.getInput()) {
            operation.setInput(new ArrayList<>());
        }

        if (null == operation.getMatchKey()) {
            if (!operation.getJoinType().equals(JoinType.INNER)) {
                throw new OperationException("You must specify an Iterable side to match on");
            }
            // setting match key to avoid swapping inputs
            operation.setMatchKey(MatchKey.LEFT);
        }



        JoinFunction joinFunction = operation.getJoinType().createInstance();

        updateOperationInput(operation.getOperation(), null);
        Iterable<I> rightIterable =
                (Iterable<I>) getResultsOrNull(operation.getOperation(),
                        context,
                        store);

        final Iterable leftIterable;
        final List rightList;

        try {
            if (operation.getMatchKey().equals(MatchKey.LEFT)) {
                leftIterable = new LimitedCloseableIterable(operation.getInput(), 0, limit, false);
                rightList = Lists.newArrayList(new LimitedCloseableIterable(rightIterable, 0, limit, false));
            } else {
                leftIterable = new LimitedCloseableIterable(rightIterable, 0, limit, false);
                rightList = Lists.newArrayList(new LimitedCloseableIterable(operation.getInput(), 0, limit, false));
            }
        } catch (final LimitExceededException e) {
            throw new OperationException(e);
        }

        final Iterable<MapTuple> joinResults = joinFunction.join(leftIterable, rightList, operation.getMatchMethod(), operation.getMatchKey());


        if (!operation.isFlatten()) {
            return unflattenResults(joinResults);
        }

        return joinResults;
    }

    private Iterable<MapTuple> unflattenResults(final Iterable joinResults) {
        return null;
    }
}
