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

package uk.gov.gchq.gaffer.accumulostore.key.core.impl.byteEntity;

import org.apache.accumulo.core.client.IteratorSetting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.accumulostore.key.core.AbstractCoreKeyIteratorSettingsFactory;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.accumulostore.utils.IteratorSettingBuilder;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.operation.graph.GraphFilters;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;

public class ByteEntityIteratorSettingsFactory extends AbstractCoreKeyIteratorSettingsFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(ByteEntityIteratorSettingsFactory.class);
    private static final String RANGE_ELEMENT_PROPERTY_FILTER_ITERATOR = ByteEntityRangeElementPropertyFilterIterator.class
            .getName();

    @Override
    public IteratorSetting getEdgeEntityDirectionFilterIteratorSetting(final GraphFilters operation) {
        return null;
    }

    @Override
    public IteratorSetting getElementPropertyRangeQueryFilter(final GraphFilters operation) {
        final boolean includeEntities = operation.getView().hasEntities();
        final boolean includeEdges = operation.getView().hasEdges();
        final DirectedType directedType = operation.getDirectedType();
        final SeededGraphFilters.IncludeIncomingOutgoingType inOutType;
        if (operation instanceof SeededGraphFilters) {
            inOutType = ((SeededGraphFilters) operation).getIncludeIncomingOutGoing();
        } else {
            inOutType = SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING;
        }
        final boolean deduplicateUndirectedEdges = operation instanceof GetAllElements;

        if (includeEdges
                && DirectedType.isEither(directedType)
                && (null == inOutType || inOutType == SeededGraphFilters.IncludeIncomingOutgoingType.EITHER)
                && includeEntities && !deduplicateUndirectedEdges) {
            LOGGER.debug("Returning null from getElementPropertyRangeQueryFilter ("
                            + "inOutType = {}, includeEdges = {}, "
                            + "directedType = {}, deduplicateUndirectedEdges = {})",
                    inOutType, includeEdges, directedType, deduplicateUndirectedEdges);
            return null;
        }

        final IteratorSetting is = new IteratorSettingBuilder(AccumuloStoreConstants.RANGE_ELEMENT_PROPERTY_FILTER_ITERATOR_PRIORITY,
                AccumuloStoreConstants.RANGE_ELEMENT_PROPERTY_FILTER_ITERATOR_NAME, RANGE_ELEMENT_PROPERTY_FILTER_ITERATOR)
                .all()
                .includeIncomingOutgoing(inOutType)
                .includeEdges(includeEdges)
                .directedType(directedType)
                .includeEntities(includeEntities)
                .deduplicateUndirectedEdges(deduplicateUndirectedEdges)
                .build();
        LOGGER.debug("Creating IteratorSetting for iterator class {} with priority = {}, "
                        + " includeIncomingOutgoing = {}, directedType = {},"
                        + " includeEdges = {}, includeEntities = {}, deduplicateUndirectedEdges = {}",
                RANGE_ELEMENT_PROPERTY_FILTER_ITERATOR,
                AccumuloStoreConstants.RANGE_ELEMENT_PROPERTY_FILTER_ITERATOR_PRIORITY,
                inOutType, directedType, includeEdges, includeEntities,
                deduplicateUndirectedEdges);
        return is;
    }

}
