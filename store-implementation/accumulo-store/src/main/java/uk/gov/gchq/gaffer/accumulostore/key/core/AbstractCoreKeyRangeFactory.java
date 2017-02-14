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
import uk.gov.gchq.gaffer.operation.GetElementsOperation;
import uk.gov.gchq.gaffer.operation.GetOperation.IncludeEdgeType;
import uk.gov.gchq.gaffer.operation.GetOperation.SeedMatchingType;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractCoreKeyRangeFactory implements RangeFactory {

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "If an element is not an Entity it must be an Edge")
    @Override
    public <T extends GetElementsOperation<?, ?>> List<Range> getRange(final ElementSeed elementSeed, final T operation)
            throws RangeFactoryException {
        if (elementSeed instanceof EntitySeed) {
            if (SeedMatchingType.EQUAL.equals(operation.getSeedMatching()) && !operation.isIncludeEntities()) {
                throw new IllegalArgumentException(
                        "When doing querying by ID, you should only provide an EntitySeed seed if you also set includeEntities flag to true");
            }
            return getRange(((EntitySeed) elementSeed).getVertex(), operation, operation.getIncludeEdges());
        } else {
            if (!operation.isIncludeEntities() && IncludeEdgeType.NONE == operation.getIncludeEdges()) {
                throw new IllegalArgumentException("Need to get either Entities and/or Edges when getting Elements");
            }

            final EdgeSeed edgeSeed = (EdgeSeed) elementSeed;
            final List<Range> ranges = new ArrayList<>();
            if (IncludeEdgeType.ALL == operation.getIncludeEdges()
                    || (IncludeEdgeType.DIRECTED == operation.getIncludeEdges() && edgeSeed.isDirected())
                    || (IncludeEdgeType.UNDIRECTED == operation.getIncludeEdges() && !edgeSeed.isDirected())) {
                // Get Edges with the given EdgeSeed - This is applicable for
                // EQUALS and RELATED seed matching.
                ranges.add(new Range(getKeyFromEdgeSeed(edgeSeed, operation, false), true,
                        getKeyFromEdgeSeed(edgeSeed, operation, true), true));
            }
            if (SeedMatchingType.RELATED.equals(operation.getSeedMatching()) && operation.isIncludeEntities()) {
                // Get Entities related to EdgeSeeds
                ranges.addAll(getRange(edgeSeed.getSource(), operation, IncludeEdgeType.NONE));
                ranges.addAll(getRange(edgeSeed.getDestination(), operation, IncludeEdgeType.NONE));
            }

            return ranges;
        }
    }

    @Override
    public <T extends GetElementsOperation<?, ?>> Range getRangeFromPair(final Pair<ElementSeed> pairRange, final T operation)
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

    protected abstract <T extends GetElementsOperation<?, ?>> Key getKeyFromEdgeSeed(final EdgeSeed seed, final T operation,
            final boolean endKey) throws RangeFactoryException;

    protected abstract <T extends GetElementsOperation<?, ?>> List<Range> getRange(final Object vertex, final T operation,
            final IncludeEdgeType includeEdgesParam) throws RangeFactoryException;
}
