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

package uk.gov.gchq.gaffer.accumulostore.key.core;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.accumulo.core.data.Range;

import uk.gov.gchq.gaffer.accumulostore.key.RangeFactory;
import uk.gov.gchq.gaffer.accumulostore.key.exception.RangeFactoryException;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.operation.SeedMatching;
import uk.gov.gchq.gaffer.operation.graph.GraphFilters;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractCoreKeyRangeFactory implements RangeFactory {

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "If an element is not an Entity it must be an Edge")
    @Override
    public List<Range> getRange(final ElementId elementId, final GraphFilters operation)
            throws RangeFactoryException {
        final SeededGraphFilters.IncludeIncomingOutgoingType inOutType = (operation instanceof SeededGraphFilters) ? ((SeededGraphFilters) operation).getIncludeIncomingOutGoing() : SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING;
        return getRange(elementId, operation, inOutType);
    }

    private List<Range> getRange(final ElementId elementId, final GraphFilters operation, final SeededGraphFilters.IncludeIncomingOutgoingType inOutType)
            throws RangeFactoryException {
        if (elementId instanceof EntityId) {
            return getRange(((EntityId) elementId).getVertex(), operation, operation.getView().hasEdges());
        } else {
            final EdgeId edgeId = (EdgeId) elementId;
            final List<Range> ranges = new ArrayList<>();
            if (operation.getView().hasEdges()
                    && DirectedType.areCompatible(operation.getDirectedType(), edgeId.getDirectedType())) {
                // EQUALS and RELATED seed matching.
                final DirectedType directed = DirectedType.and(operation.getDirectedType(), edgeId.getDirectedType());
                ranges.addAll(getRange(edgeId.getSource(), edgeId.getDestination(), directed, operation, inOutType));
            }

            // Do related - if operation doesn't have seed matching or it has seed matching equal to RELATED
            final boolean doRelated = !(operation instanceof SeedMatching)
                    || !SeedMatching.SeedMatchingType.EQUAL.equals(((SeedMatching) operation).getSeedMatching());
            if (doRelated && operation.getView().hasEntities()) {
                // Get Entities related to EdgeIds
                ranges.addAll(getRange(edgeId.getSource(), operation, false));
                ranges.addAll(getRange(edgeId.getDestination(), operation, false));
            }

            return ranges;
        }
    }

    @Override
    public Range getRangeFromPair(final Pair<ElementId, ElementId> pairRange, final GraphFilters operation)
            throws RangeFactoryException {
        final ArrayList<Range> ran = new ArrayList<>();
        // set the in out flag to null to disable it
        ran.addAll(getRange(pairRange.getFirst(), operation, null));
        ran.addAll(getRange(pairRange.getSecond(), operation, null));
        Range min = null;
        Range max = null;
        for (final Range range : ran) {
            if (null == min) {
                min = range;
                max = range;
            }
            if (range.compareTo(min) < 0) {
                min = range;
            } else if (range.compareTo(max) > 0) {
                max = range;
            }
        }
        return new Range(min.getStartKey(), max.getEndKey());
    }

    protected abstract List<Range> getRange(final Object sourceVal, final Object destVal, final DirectedType directed,
                                            final GraphFilters operation, SeededGraphFilters.IncludeIncomingOutgoingType inOutType) throws RangeFactoryException;

    protected abstract List<Range> getRange(final Object vertex,
                                            final GraphFilters operation,
                                            final boolean includeEdges) throws RangeFactoryException;
}
