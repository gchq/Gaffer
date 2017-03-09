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

package uk.gov.gchq.gaffer.store.operation.handler;

import uk.gov.gchq.gaffer.commonutil.iterable.LimitedCloseableIterable;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.Limit;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

/**
 * An <code>LimitHandler</code> handles for {@link Limit} operations.
 * It simply wraps the input iterable in a
 * {@link uk.gov.gchq.gaffer.commonutil.iterable.LimitedCloseableIterable} so the data is
 * not stored in memory.
 */
public class LimitHandler<T> implements OperationHandler<Limit<T>, Iterable<T>> {
    @Override
    public Iterable<T> doOperation(final Limit<T> operation, final Context context, final Store store) throws OperationException {
        return null != operation.getResultLimit()
                ? new LimitedCloseableIterable<>(operation.getInput(), 0, operation.getResultLimit())
                : operation.getInput();
    }
}
