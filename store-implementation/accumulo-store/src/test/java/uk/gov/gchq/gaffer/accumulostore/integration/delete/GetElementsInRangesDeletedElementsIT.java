/*
 * Copyright 2017-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.accumulostore.integration.delete;

import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsInRanges;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters.IncludeIncomingOutgoingType;

import java.util.ArrayList;
import java.util.List;

public class GetElementsInRangesDeletedElementsIT extends AbstractDeletedElementsIT<GetElementsInRanges, CloseableIterable<? extends Element>> {
    @Override
    protected GetElementsInRanges createGetOperation() {
        final List<Pair<ElementId, ElementId>> pairs = new ArrayList<>();
        for (final String vertex : VERTICES) {
            pairs.add(new Pair<>(new EntitySeed(vertex), new EntitySeed(vertex + "z")));
        }

        return new GetElementsInRanges.Builder()
                .input(pairs)
                .inOutType(IncludeIncomingOutgoingType.OUTGOING)
                .build();
    }
}
