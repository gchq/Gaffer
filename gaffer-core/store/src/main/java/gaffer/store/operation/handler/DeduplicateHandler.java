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

package gaffer.store.operation.handler;

import com.google.common.collect.Sets;
import gaffer.operation.OperationException;
import gaffer.operation.impl.Deduplicate;
import gaffer.store.Context;
import gaffer.store.Store;

/**
 * An <code>DeduplicateHandler</code> handles for {@link Deduplicate} operations.
 * Adds all the operation input items into a {@link java.util.LinkedHashSet} to
 * remove duplicate items.
 */
public class DeduplicateHandler<T> implements OperationHandler<Deduplicate<T>, Iterable<T>> {
    @Override
    public Iterable<T> doOperation(final Deduplicate<T> operation, final Context context, final Store store) throws OperationException {
        return Sets.newLinkedHashSet(operation.getInput());
    }
}
