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

package gaffer.accumulostore.operation.handler;

import com.google.common.collect.Sets;
import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.utils.AccumuloStoreConstants;
import gaffer.operation.Operation;
import gaffer.operation.OperationException;
import gaffer.store.Context;
import gaffer.store.Store;
import gaffer.store.operation.handler.OperationHandler;
import gaffer.user.User;

public abstract class AccumuloGetIterableHandler<OPERATION extends Operation<?, Iterable<OUTPUT_ITEM>>, OUTPUT_ITEM> implements OperationHandler<OPERATION, Iterable<OUTPUT_ITEM>> {

    @Override
    public Iterable<OUTPUT_ITEM> doOperation(final OPERATION operation, final Context context, final Store store)
            throws OperationException {
        Iterable<OUTPUT_ITEM> results = doOperation(operation, context.getUser(), (AccumuloStore) store);
        if (Boolean.parseBoolean(operation.getOption(AccumuloStoreConstants.DEDUPLICATE_RESULTS))) {
            results = Sets.newLinkedHashSet(results);
        }

        return results;
    }

    protected abstract Iterable<OUTPUT_ITEM> doOperation(final OPERATION operation, final User user, final AccumuloStore store) throws OperationException;
}
