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

package uk.gov.gchq.gaffer.accumulostore.retriever.impl;

import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.exception.IteratorSettingException;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.user.User;

import java.util.stream.StreamSupport;

public class AccumuloElementsRetriever extends AccumuloSingleIDRetriever {

    @SuppressWarnings("unchecked")
    public AccumuloElementsRetriever(final AccumuloStore store,
                                     final Operation operation,
                                     final View view,
                                     final DirectedType directedType,
                                     final User user)
            throws IteratorSettingException, StoreException {
        super(store, operation, view, user,
                // includeMatchedVertex if input only contains EntityIds
                StreamSupport.stream(Iterable.class.cast(operation.getInput()).spliterator(), false).noneMatch(input -> EdgeId.class.isInstance(input)),
                store.getKeyPackage().getIteratorFactory().getElementPreAggregationFilterIteratorSetting(view, store),
                store.getKeyPackage().getIteratorFactory().getElementPostAggregationFilterIteratorSetting(view, store),
                store.getKeyPackage().getIteratorFactory().getEdgeEntityDirectionFilterIteratorSetting(operation, view, directedType),
                store.getKeyPackage().getIteratorFactory().getQueryTimeAggregatorIteratorSetting(view, store));
    }
}
