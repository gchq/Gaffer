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

package uk.gov.gchq.gaffer.accumulostore.key.core.impl.byteEntity;

import org.apache.accumulo.core.client.IteratorSetting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.accumulostore.key.core.AbstractCoreKeyIteratorSettingsFactory;
import uk.gov.gchq.gaffer.accumulostore.utils.IteratorSettingBuilder;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;

import static java.util.Objects.isNull;

import static uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants.RANGE_ELEMENT_PROPERTY_FILTER_ITERATOR_NAME;
import static uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants.RANGE_ELEMENT_PROPERTY_FILTER_ITERATOR_PRIORITY;

public class ByteEntityIteratorSettingsFactory extends AbstractCoreKeyIteratorSettingsFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(ByteEntityIteratorSettingsFactory.class);

    private static final String RANGE_ELEMENT_PROPERTY_FILTER_ITERATOR = ByteEntityRangeElementPropertyFilterIterator.class.getName();

    private static final String LOG_RETURNING_NULL = "Returning null from getElementPropertyRangeQueryFilter ("
            + "inOutType = {}, includeEdges = {}, directedType = {}, deduplicateUndirectedEdges = {})";

    private static final String LOG_CREATING_ITERATOR_SETTING = "Creating IteratorSetting for iterator class {} with priority = {}, "
            + " includeIncomingOutgoing = {}, directedType = {}, includeEdges = {}, includeEntities = {}, deduplicateUndirectedEdges = {}";

    @Override
    public IteratorSetting getEdgeEntityDirectionFilterIteratorSetting(final Operation operation, final View view,
                                                                       final DirectedType directedType) {
        return null;
    }

    @Override
    public IteratorSetting getElementPropertyRangeQueryFilter(final Operation operation, final View view,
                                                              final DirectedType directedType) {
        final boolean includeEntities = view.hasEntities();
        final boolean includeEdges = view.hasEdges();
        final SeededGraphFilters.IncludeIncomingOutgoingType inOutType;
        if (operation instanceof SeededGraphFilters) {
            inOutType = ((SeededGraphFilters) operation).getIncludeIncomingOutGoing();
        } else {
            inOutType = SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING;
        }

        // TODO: GetAllElements correct?
        final boolean deduplicateUndirectedEdges = operation.getIdComparison("GetAllElements");

        if (includeEdges
                && DirectedType.isEither(directedType)
                && (isNull(inOutType) || inOutType == SeededGraphFilters.IncludeIncomingOutgoingType.EITHER)
                && includeEntities && !deduplicateUndirectedEdges) {
            LOGGER.debug(LOG_RETURNING_NULL, inOutType, includeEdges, directedType, deduplicateUndirectedEdges);
            return null;
        }

        final IteratorSetting is = new IteratorSettingBuilder(RANGE_ELEMENT_PROPERTY_FILTER_ITERATOR_PRIORITY,
                RANGE_ELEMENT_PROPERTY_FILTER_ITERATOR_NAME, RANGE_ELEMENT_PROPERTY_FILTER_ITERATOR)
                        .all()
                        .includeIncomingOutgoing(inOutType)
                        .includeEdges(includeEdges)
                        .directedType(directedType)
                        .includeEntities(includeEntities)
                        .deduplicateUndirectedEdges(deduplicateUndirectedEdges)
                        .build();
        LOGGER.debug(LOG_CREATING_ITERATOR_SETTING,
                RANGE_ELEMENT_PROPERTY_FILTER_ITERATOR,
                RANGE_ELEMENT_PROPERTY_FILTER_ITERATOR_PRIORITY,
                inOutType, directedType, includeEdges, includeEntities,
                deduplicateUndirectedEdges);
        return is;
    }
}
