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
package uk.gov.gchq.gaffer.store.operation.handler.output;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.output.ToArray;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.List;

public class ToArrayHandler<T> implements OutputOperationHandler<ToArray<T>, T[]> {
    @SuppressFBWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS")
    @Override
    public T[] doOperation(final ToArray<T> operation, final Context context, final Store store) throws OperationException {
        if (null == operation.getInput() || Iterables.isEmpty(operation.getInput())) {
            return null;
        }

        return toArray(operation.getInput());
    }

    private <T> T[] toArray(final Collection<T> collection, final Class<?> clazz) throws OperationException {
        return toArray(collection, (T[]) Array.newInstance(clazz, collection.size()));
    }

    private <T> T[] toArray(final Iterable<T> iterable) throws OperationException {
        final List<T> list = Lists.newArrayList(iterable);
        Class<?> clazz = Object.class;
        if (!list.isEmpty()) {
            clazz = list.get(0).getClass();
        }
        return toArray(Lists.newArrayList(iterable), clazz);
    }

    private <T> T[] toArray(final Collection<T> collection, final T[] array) throws OperationException {
        return collection.toArray(array);
    }

}
