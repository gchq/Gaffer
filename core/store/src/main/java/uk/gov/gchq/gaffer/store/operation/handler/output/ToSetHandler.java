/*
 * Copyright 2016-2018 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.stream.GafferCollectors;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.output.ToSet;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

import java.util.Set;

/**
 * The {@code ToSetHandler} handles {@link ToSet} operations by collecting the
 * items in the input {@link Iterable} into a {@link Set}, removing duplicate items
 * in the process.
 *
 * Use of this operation will cause all of the items present in the input iterable
 * to be brought into memory, so this operation is not suitable for situations where
 * the size of the input iterable is very large.
 *
 * @param <T> the type of object contained in the input iterable
 */
public class ToSetHandler<T> implements OutputOperationHandler<ToSet<T>, Set<? extends T>> {
    @Override
    public Set<T> doOperation(final ToSet<T> operation, final Context context, final Store store) throws OperationException {
        if (null == operation.getInput()) {
            return null;
        }

        return Streams.toStream(operation.getInput())
                      .collect(GafferCollectors.toLinkedHashSet());
    }
}
