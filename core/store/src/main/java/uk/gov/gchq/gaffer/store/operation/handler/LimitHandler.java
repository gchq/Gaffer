/*
 * Copyright 2016-2020 Crown Copyright
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
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

/**
 * An {@code LimitHandler} handles for {@link Limit} operations.
 * It simply wraps the input iterable in a
 * {@link uk.gov.gchq.gaffer.commonutil.iterable.LimitedCloseableIterable} so the data is
 * not stored in memory.
 */
public class LimitHandler implements OperationHandler<Iterable> {

    public static final String TRUNCATE = "truncate";
    public static final String RESULT_LIMIT = "resultLimit";

    @Override
    public Iterable _doOperation(Operation operation, Context context, Store store) throws OperationException {
        Iterable input = (Iterable) operation.input();
        Integer resultLimit = (Integer) operation.get(RESULT_LIMIT);
        Boolean truncate = (Boolean) operation.get(TRUNCATE);

        return new LimitedCloseableIterable(input, 0, resultLimit, truncate);
    }

    @Override
    public FieldDeclaration getFieldDeclaration() {
        return new FieldDeclaration()
                .fieldRequired("input", Iterable.class)
                .fieldRequired(TRUNCATE, Boolean.class)
                .fieldRequired(RESULT_LIMIT, Integer.class);
    }
}
