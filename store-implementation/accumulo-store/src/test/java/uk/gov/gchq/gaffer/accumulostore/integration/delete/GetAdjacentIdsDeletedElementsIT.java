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

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters.IncludeIncomingOutgoingType;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;

import java.util.ArrayList;
import java.util.List;

public class GetAdjacentIdsDeletedElementsIT extends AbstractDeletedElementsIT<GetAdjacentIds, CloseableIterable<? extends EntityId>> {
    @Override
    protected GetAdjacentIds createGetOperation() {
        return new GetAdjacentIds.Builder()
                .input(VERTICES)
                .inOutType(IncludeIncomingOutgoingType.OUTGOING)
                .build();
    }

    @Override
    protected void assertElements(final Iterable<ElementId> expected, final CloseableIterable<? extends EntityId> actual) {
        final List<ElementId> expectedIds = new ArrayList<>();
        for (final ElementId element : expected) {
            if (element instanceof EdgeId) {
                expectedIds.add(new EntitySeed(((EdgeId) element).getDestination()));
            }
        }
        super.assertElements(expectedIds, actual);
    }
}
