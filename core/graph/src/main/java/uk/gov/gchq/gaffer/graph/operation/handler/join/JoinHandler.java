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

package uk.gov.gchq.gaffer.graph.operation.handler.join;

import com.google.common.collect.Lists;

import uk.gov.gchq.gaffer.commonutil.exception.LimitExceededException;
import uk.gov.gchq.gaffer.commonutil.iterable.LimitedCloseableIterable;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.graph.operation.handler.join.merge.ElementMerge;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.join.Join;
import uk.gov.gchq.gaffer.operation.impl.join.methods.JoinFunction;
import uk.gov.gchq.gaffer.operation.impl.join.methods.JoinType;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

import java.util.ArrayList;
import java.util.List;

import static uk.gov.gchq.gaffer.store.operation.handler.util.OperationHandlerUtil.getResultsOrNull;
import static uk.gov.gchq.gaffer.store.operation.handler.util.OperationHandlerUtil.updateOperationInput;

public class JoinHandler<I, O> implements OutputOperationHandler<Join<I, O>, Iterable<? extends O>> {
    @Override
    public Iterable<? extends O> doOperation(final Join<I, O> operation, final Context context, final Store store) throws OperationException {
        final int limit = operation.getCollectionLimit() != null ? operation.getCollectionLimit() : 100000;

        if (null == operation.getJoinType()) {
            throw new OperationException("A join type must be specified");
        }

        if (null == operation.getMergeMethod()) {
            throw new OperationException("A merge method must be specified");
        }

        if (null == operation.getInput()) {
            operation.setInput(new ArrayList<>());
        }

        if (null == operation.getMatchKey() && (!operation.getJoinType().equals(JoinType.FULL_OUTER))) {
            throw new OperationException("You must specify an Iterable side to match on");
        }

        JoinFunction joinFunction = operation.getJoinType().createInstance();

        updateOperationInput(operation.getOperation(), null);
        Iterable<I> rightIterable =
                (Iterable<I>) getResultsOrNull(operation.getOperation(),
                        context,
                        store);

        final List leftList;
        final List rightList;

        try {
            leftList = Lists.newArrayList(new LimitedCloseableIterable(operation.getInput(), 0, limit, false));
            rightList = Lists.newArrayList(new LimitedCloseableIterable(rightIterable, 0, limit, false));
        } catch (final LimitExceededException e) {
            throw new OperationException(e);
        }

        final Iterable joinResults = joinFunction.join(leftList, rightList, operation.getMatchMethod(), operation.getMatchKey());

        if (operation.getMergeMethod() instanceof ElementMerge) {
            ((ElementMerge) operation.getMergeMethod()).setSchema(((GraphConfig) store.getConfig()).getSchema());
        }
        return operation.getMergeMethod().merge(joinResults);
    }
}
