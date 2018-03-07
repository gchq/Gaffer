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
package uk.gov.gchq.gaffer.accumulostore.key.core.impl.classic;

import org.apache.accumulo.core.client.IteratorSetting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.accumulostore.key.core.AbstractCoreKeyIteratorSettingsFactory;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.accumulostore.utils.IteratorSettingBuilder;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.operation.graph.GraphFilters;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters.IncludeIncomingOutgoingType;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;

public class ClassicIteratorSettingsFactory extends AbstractCoreKeyIteratorSettingsFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClassicIteratorSettingsFactory.class);
    private static final String EDGE_DIRECTED_UNDIRECTED_FILTER = ClassicEdgeDirectedUndirectedFilterIterator.class
            .getName();
    private static final String RANGE_ELEMENT_PROPERTY_FILTER_ITERATOR = ClassicRangeElementPropertyFilterIterator.class
            .getName();

    @Override
    public IteratorSetting getEdgeEntityDirectionFilterIteratorSetting(final GraphFilters operation) {
        final boolean includeEntities = operation.getView().hasEntities();
        final boolean includeEdges = operation.getView().hasEdges();
        final DirectedType directedType = operation.getDirectedType();
        final IncludeIncomingOutgoingType inOutType;
        if (operation instanceof SeededGraphFilters) {
            inOutType = ((SeededGraphFilters) operation).getIncludeIncomingOutGoing();
        } else {
            inOutType = IncludeIncomingOutgoingType.OUTGOING;
        }
        final boolean deduplicateUndirectedEdges = operation instanceof GetAllElements;

        if ((null == inOutType || inOutType == IncludeIncomingOutgoingType.EITHER)
                && includeEdges
                && (DirectedType.isEither(directedType))
                && !deduplicateUndirectedEdges) {
            LOGGER.debug("Returning null from getEdgeEntityDirectionFilterIteratorSetting ("
                            + "inOutType = {}, includeEdges = {}, directedType = {}, deduplicateUndirectedEdges = {})",
                    inOutType, includeEdges, directedType, deduplicateUndirectedEdges);
            return null;
        }

        final IteratorSetting is = new IteratorSettingBuilder(
                AccumuloStoreConstants.EDGE_ENTITY_DIRECTED_UNDIRECTED_INCOMING_OUTGOING_FILTER_ITERATOR_PRIORITY,
                AccumuloStoreConstants.EDGE_ENTITY_DIRECTED_UNDIRECTED_INCOMING_OUTGOING_FILTER_ITERATOR_NAME,
                EDGE_DIRECTED_UNDIRECTED_FILTER)
                .includeIncomingOutgoing(inOutType)
                .directedType(directedType)
                .includeEdges(includeEdges)
                .includeEntities(includeEntities)
                .deduplicateUndirectedEdges(deduplicateUndirectedEdges)
                .build();
        LOGGER.debug("Creating IteratorSetting for iterator class {} with "
                + "priority = {}, includeIncomingOutgoing = {}, directedType = {}, "
                + "includeEdges = {}, includeEntities = {}, deduplicateUndirectedEdges = {}",
                EDGE_DIRECTED_UNDIRECTED_FILTER, AccumuloStoreConstants.EDGE_ENTITY_DIRECTED_UNDIRECTED_INCOMING_OUTGOING_FILTER_ITERATOR_PRIORITY,
                inOutType, directedType, includeEdges, includeEntities,
                deduplicateUndirectedEdges);
        return is;
    }

    @Override
    public IteratorSetting getElementPropertyRangeQueryFilter(final GraphFilters operation) {
        final boolean includeEntities = operation.getView().hasEntities();
        final boolean includeEdges = operation.getView().hasEdges();
        if (includeEntities && includeEdges) {
            LOGGER.debug("Returning null from getElementPropertyRangeQueryFilter as includeEntities = {} and includeEdges = {}",
                    includeEntities, includeEdges);
            return null;
        }

        final IteratorSetting is = new IteratorSettingBuilder(
                AccumuloStoreConstants.RANGE_ELEMENT_PROPERTY_FILTER_ITERATOR_PRIORITY,
                AccumuloStoreConstants.RANGE_ELEMENT_PROPERTY_FILTER_ITERATOR_NAME, RANGE_ELEMENT_PROPERTY_FILTER_ITERATOR).all()
                .includeEdges(includeEdges)
                .includeEntities(includeEntities)
                .build();
        LOGGER.debug("Creating IteratorSetting for iterator class {} with priority = {}, "
                + "includeEdges = {}, includeEntities = {}",
                RANGE_ELEMENT_PROPERTY_FILTER_ITERATOR,
                AccumuloStoreConstants.RANGE_ELEMENT_PROPERTY_FILTER_ITERATOR_PRIORITY,
                includeEdges, includeEntities);
        return is;
    }

}
