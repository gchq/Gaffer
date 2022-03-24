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
package uk.gov.gchq.gaffer.accumulostore.key.core.impl.classic;

import org.apache.accumulo.core.client.IteratorSetting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.accumulostore.key.core.AbstractCoreKeyIteratorSettingsFactory;
import uk.gov.gchq.gaffer.accumulostore.utils.IteratorSettingBuilder;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters.IncludeIncomingOutgoingType;

import static java.util.Objects.isNull;

import static uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants.EDGE_ENTITY_DIRECTED_UNDIRECTED_INCOMING_OUTGOING_FILTER_ITERATOR_NAME;
import static uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants.EDGE_ENTITY_DIRECTED_UNDIRECTED_INCOMING_OUTGOING_FILTER_ITERATOR_PRIORITY;
import static uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants.RANGE_ELEMENT_PROPERTY_FILTER_ITERATOR_NAME;
import static uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants.RANGE_ELEMENT_PROPERTY_FILTER_ITERATOR_PRIORITY;

public class ClassicIteratorSettingsFactory extends AbstractCoreKeyIteratorSettingsFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClassicIteratorSettingsFactory.class);

    private static final String EDGE_DIRECTED_UNDIRECTED_FILTER = ClassicEdgeDirectedUndirectedFilterIterator.class.getName();

    private static final String RANGE_ELEMENT_PROPERTY_FILTER_ITERATOR = ClassicRangeElementPropertyFilterIterator.class.getName();

    private static final String LOG_CREATING_BASIC_ITERATOR_SETTING = "Creating IteratorSetting for iterator class {} with priority = {}, "
            + "includeEdges = {}, includeEntities = {}";

    private static final String LOG_CREATING_ITERATOR_SETTING = "Creating IteratorSetting for iterator class {} with priority = {}, "
            + "includeIncomingOutgoing = {}, directedType = {}, includeEdges = {}, includeEntities = {}, deduplicateUndirectedEdges = {}";

    private static final String LOG_RETURNING_NULL_FROM_GET_ELEMENT = "Returning null from getElementPropertyRangeQueryFilter as "
            + "includeEntities = {} and includeEdges = {}";

    private static final String LOG_RETURNING_NULL_FROM_GET_EDGE = "Returning null from getEdgeEntityDirectionFilterIteratorSetting ("
            + "inOutType = {}, includeEdges = {}, directedType = {}, deduplicateUndirectedEdges = {})";

    @Override
    public IteratorSetting getEdgeEntityDirectionFilterIteratorSetting(final Operation operation, final View view,
                                                                       final DirectedType directedType) {
        final boolean includeEntities = view.hasEntities();
        final boolean includeEdges = view.hasEdges();
        final IncludeIncomingOutgoingType inOutType;
        if (operation instanceof SeededGraphFilters) {
            inOutType = ((SeededGraphFilters) operation).getIncludeIncomingOutGoing();
        } else {
            inOutType = IncludeIncomingOutgoingType.OUTGOING;
        }

        // TODO: GetAllElements correct?
        final boolean deduplicateUndirectedEdges = operation.getIdComparison("GetAllElements");

        if ((isNull(inOutType) || inOutType == IncludeIncomingOutgoingType.EITHER)
                && includeEdges
                && DirectedType.isEither(directedType)
                && !deduplicateUndirectedEdges) {
            LOGGER.debug(LOG_RETURNING_NULL_FROM_GET_EDGE, inOutType, includeEdges, directedType, deduplicateUndirectedEdges);
            return null;
        }

        final IteratorSetting is = new IteratorSettingBuilder(EDGE_ENTITY_DIRECTED_UNDIRECTED_INCOMING_OUTGOING_FILTER_ITERATOR_PRIORITY,
                EDGE_ENTITY_DIRECTED_UNDIRECTED_INCOMING_OUTGOING_FILTER_ITERATOR_NAME,
                EDGE_DIRECTED_UNDIRECTED_FILTER)
                        .includeIncomingOutgoing(inOutType)
                        .directedType(directedType)
                        .includeEdges(includeEdges)
                        .includeEntities(includeEntities)
                        .deduplicateUndirectedEdges(deduplicateUndirectedEdges)
                        .build();
        LOGGER.debug(LOG_CREATING_ITERATOR_SETTING,
                EDGE_DIRECTED_UNDIRECTED_FILTER, EDGE_ENTITY_DIRECTED_UNDIRECTED_INCOMING_OUTGOING_FILTER_ITERATOR_PRIORITY,
                inOutType, directedType, includeEdges, includeEntities,
                deduplicateUndirectedEdges);
        return is;
    }

    @Override
    public IteratorSetting getElementPropertyRangeQueryFilter(final Operation operation, final View view, final DirectedType directedType) {
        final boolean includeEntities = view.hasEntities();
        final boolean includeEdges = view.hasEdges();
        if (includeEntities && includeEdges) {
            LOGGER.debug(LOG_RETURNING_NULL_FROM_GET_ELEMENT,
                    includeEntities, includeEdges);
            return null;
        }

        final IteratorSetting is = new IteratorSettingBuilder(RANGE_ELEMENT_PROPERTY_FILTER_ITERATOR_PRIORITY,
                RANGE_ELEMENT_PROPERTY_FILTER_ITERATOR_NAME, RANGE_ELEMENT_PROPERTY_FILTER_ITERATOR).all()
                        .includeEdges(includeEdges)
                        .includeEntities(includeEntities)
                        .build();
        LOGGER.debug(LOG_CREATING_BASIC_ITERATOR_SETTING,
                RANGE_ELEMENT_PROPERTY_FILTER_ITERATOR,
                RANGE_ELEMENT_PROPERTY_FILTER_ITERATOR_PRIORITY,
                includeEdges, includeEntities);
        return is;
    }
}
