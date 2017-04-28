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

package uk.gov.gchq.gaffer.doc.user.walkthrough;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.google.common.collect.Iterables;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.operation.OperationException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class CardinalitiesTest {
    @Test
    public void shouldReturnExpectedEdges() throws OperationException {
        // Given
        final Cardinalities query = new Cardinalities();

        // When
        final CloseableIterable<? extends Element> results = query.run();

        // Then
        verifyResults(results);
    }

    private void verifyResults(final CloseableIterable<? extends Element> resultsItr) {
        final Map<String, Entity> expectedResults = new HashMap<>();
        expectedResults.put("RoadUse",
                new Entity.Builder()
                        .group("Cardinality")
                        .vertex("10")
                        .property("count", 8)
                        .property("edgeGroup", CollectionUtil.treeSet(new String[]{"RoadUse", "RoadHasJunction" }))
                        .property("hllp", 4L)
                        .build()
        );

        assertEquals(expectedResults.size(), Iterables.size(resultsItr));
        for (final Element entity : resultsItr) {
            // HyperLogLogPlus has not overridden the equals method so this is a work around to check the cardinality values are the same.
            entity.putProperty("hllp", ((HyperLogLogPlus) entity.getProperty("hllp")).cardinality());
            assertEquals(expectedResults.get(((Entity) entity).getVertex().toString()), entity);
        }
    }
}