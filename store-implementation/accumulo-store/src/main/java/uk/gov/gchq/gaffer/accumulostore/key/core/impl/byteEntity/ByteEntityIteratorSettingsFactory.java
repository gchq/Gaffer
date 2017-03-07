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

package uk.gov.gchq.gaffer.accumulostore.key.core.impl.byteEntity;

import org.apache.accumulo.core.client.IteratorSetting;
import uk.gov.gchq.gaffer.accumulostore.key.core.AbstractCoreKeyIteratorSettingsFactory;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.accumulostore.utils.IteratorSettingBuilder;
import uk.gov.gchq.gaffer.operation.GetElementsOperation;
import uk.gov.gchq.gaffer.operation.GetOperation.IncludeEdgeType;
import uk.gov.gchq.gaffer.operation.GetOperation.IncludeIncomingOutgoingType;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;

public class ByteEntityIteratorSettingsFactory extends AbstractCoreKeyIteratorSettingsFactory {
    private static final String RANGE_ELEMENT_PROPERTY_FILTER_ITERATOR = ByteEntityRangeElementPropertyFilterIterator.class
            .getName();

    @Override
    public IteratorSetting getEdgeEntityDirectionFilterIteratorSetting(final GetElementsOperation<?, ?> operation) {
        return null;
    }

    @Override
    public IteratorSetting getElementPropertyRangeQueryFilter(final GetElementsOperation<?, ?> operation) {
        final boolean includeEntities = operation.isIncludeEntities();
        final IncludeEdgeType includeEdgeType = operation.getIncludeEdges();
        final IncludeIncomingOutgoingType includeIncomingOutgoingType = operation.getIncludeIncomingOutGoing();
        final boolean deduplicateUndirectedEdges = operation instanceof GetAllElements;

        if (includeEdgeType == IncludeEdgeType.ALL && includeIncomingOutgoingType == IncludeIncomingOutgoingType.BOTH
                && includeEntities && !deduplicateUndirectedEdges) {
            return null;
        }

        return new IteratorSettingBuilder(AccumuloStoreConstants.RANGE_ELEMENT_PROPERTY_FILTER_ITERATOR_PRIORITY,
                AccumuloStoreConstants.RANGE_ELEMENT_PROPERTY_FILTER_ITERATOR_NAME, RANGE_ELEMENT_PROPERTY_FILTER_ITERATOR)
                .all()
                .includeIncomingOutgoing(includeIncomingOutgoingType)
                .includeEdges(includeEdgeType)
                .includeEntities(includeEntities)
                .deduplicateUndirectedEdges(deduplicateUndirectedEdges)
                .build();
    }

}
