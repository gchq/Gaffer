/*
 * Copyright 2017-2021 Crown Copyright
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

import com.google.common.collect.Sets;

import uk.gov.gchq.gaffer.commonutil.iterable.ChainedIterable;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;

import java.util.HashSet;
import java.util.Set;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.collections.CollectionUtils.isNotEmpty;

public class GetTraitsHandler implements OperationHandler<GetTraits, Set<StoreTrait>> {
    private final Set<StoreTrait> storeTraits;
    private Set<StoreTrait> currentTraits;

    public GetTraitsHandler(final Set<StoreTrait> storeTraits) {
        this.storeTraits = Collections.unmodifiableSet(Sets.newHashSet(storeTraits));
    }

    @Override
    public Set<StoreTrait> doOperation(final Operation operation, final Context context, final Store store) throws OperationException {
        if (operation.isCurrentTraits()) {
            if (isNull(currentTraits)) {
                currentTraits = Collections.unmodifiableSet(createCurrentTraits(store));
            }
            rtn = currentTraits;
        } else {
            rtn = storeTraits
        }
        return rtn;
    }

    private Set<StoreTrait> createCurrentTraits(final Store store) {
        final Set<StoreTrait> traits = Sets.newHashSet(storeTraits);
        final Schema schema = store.getSchema();

        final boolean hasAggregatedGroups = isNotEmpty(schema.getAggregatedGroups());
        final boolean hasVisibility = nonNull(schema.getVisibilityProperty());
        boolean hasGroupBy = false;
        boolean hasValidation = false;
        for (final SchemaElementDefinition def : new ChainedIterable<SchemaElementDefinition>(schema.getEntities().values(), schema.getEdges().values())) {
            hasValidation = hasValidation || def.hasValidation();
            hasGroupBy = hasGroupBy || isNotEmpty(def.getGroupBy());
            if (hasGroupBy && hasValidation) {
                break;
            }
        }

        if (!hasAggregatedGroups) {
            traits.remove(StoreTrait.INGEST_AGGREGATION);
            traits.remove(StoreTrait.QUERY_AGGREGATION);
        }
        if (!hasGroupBy && traits.contains(StoreTrait.INGEST_AGGREGATION)) {
            traits.remove(StoreTrait.QUERY_AGGREGATION);
        }
        if (!hasValidation) {
            traits.remove(StoreTrait.STORE_VALIDATION);
        }
        if (!hasVisibility) {
            traits.remove(StoreTrait.VISIBILITY);
        }

        return traits;
    }
}
