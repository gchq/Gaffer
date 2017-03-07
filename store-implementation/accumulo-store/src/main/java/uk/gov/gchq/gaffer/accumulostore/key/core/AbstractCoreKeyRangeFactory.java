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

package uk.gov.gchq.gaffer.accumulostore.key.core;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import uk.gov.gchq.gaffer.accumulostore.key.RangeFactory;
import uk.gov.gchq.gaffer.accumulostore.key.exception.RangeFactoryException;
import uk.gov.gchq.gaffer.accumulostore.utils.Pair;
import uk.gov.gchq.gaffer.operation.SeedMatching;
import uk.gov.gchq.gaffer.operation.SeededGraphGet;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.GraphFilters;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractCoreKeyRangeFactory implements RangeFactory {

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "If an element is not an Entity it must be an Edge")
    @Override
    public <T extends SeededGraphGet<?, ?>> List<Range> getRange(final ElementSeed elementSeed, final T operation)
            throws RangeFactoryException {
        if (elementSeed instanceof EntitySeed) {
            return getRange(((EntitySeed) elementSeed).getVertex(), operation, operation.getView().hasEdges());
        } else {
            final EdgeSeed edgeSeed = (EdgeSeed) elementSeed;
            final List<Range> ranges = new ArrayList<>();
            if (operation.getView().hasEdges()
                    && (GraphFilters.DirectedType.BOTH == operation.getDirectedType()
                    || (GraphFilters.DirectedType.DIRECTED == operation.getDirectedType() && edgeSeed.isDirected())
                    || (GraphFilters.DirectedType.UNDIRECTED == operation.getDirectedType() && !edgeSeed.isDirected()))) {
                // Get Edges with the given EdgeSeed - This is applicable for
                // EQUALS and RELATED seed matching.
                ranges.add(new Range(getKeyFromEdgeSeed(edgeSeed, operation, false), true,
                        getKeyFromEdgeSeed(edgeSeed, operation, true), true));
            }

            // Do related - if operation doesn't have seed matching or it has seed matching equal to RELATED
            final boolean doRelated = !(operation instanceof SeedMatching)
                    || SeedMatching.SeedMatchingType.RELATED.equals(((SeedMatching) operation).getSeedMatching());
            if (doRelated && operation.getView().hasEntities()) {
                // Get Entities related to EdgeSeeds
                ranges.addAll(getRange(edgeSeed.getSource(), operation, false));
                ranges.addAll(getRange(edgeSeed.getDestination(), operation, false));
            }

            return ranges;
        }
    }

    @Override
    public <T extends SeededGraphGet<?, ?>> Range getRangeFromPair(final Pair<ElementSeed> pairRange, final T operation)
            throws RangeFactoryException {
        final ArrayList<Range> ran = new ArrayList<>();
        ran.addAll(getRange(pairRange.getFirst(), operation));
        ran.addAll(getRange(pairRange.getSecond(), operation));
        Range min = null;
        Range max = null;
        for (final Range range : ran) {
            if (min == null) {
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

    protected abstract <T extends SeededGraphGet<?, ?>> Key getKeyFromEdgeSeed(final EdgeSeed seed, final T operation,
                                                                               final boolean endKey) throws RangeFactoryException;

    protected abstract <T extends SeededGraphGet<?, ?>> List<Range> getRange(final Object vertex, final T operation,
                                                                             final boolean includeEdges) throws RangeFactoryException;
}
