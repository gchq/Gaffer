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
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.operation.OperationException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class CardinalitiesTest {
    @Test
    public void shouldReturnExpectedEdges() throws OperationException, IOException {
        // Given
        final Cardinalities query = new Cardinalities();

        // When
        final CloseableIterable<? extends Element> results = query.run();

        // Then
        verifyResults(results);
    }

    private void verifyResults(final CloseableIterable<? extends Element> resultsItr) {
        final Entity[] expectedResults = {
                new Entity.Builder()
                        .group("Cardinality")
                        .vertex("10")
                        .property("count", 8L)
                        .property("edgeGroup", CollectionUtil.treeSet(new String[]{"RoadHasJunction", "RoadUse" }))
                        .property("hllp", 2L)
                        .build(),
                new Entity.Builder()
                        .group("Cardinality")
                        .vertex("11")
                        .property("count", 8L)
                        .property("edgeGroup", CollectionUtil.treeSet(new String[]{"RoadHasJunction", "RoadUse" }))
                        .property("hllp", 2L)
                        .build(),
                new Entity.Builder()
                        .group("Cardinality")
                        .vertex("23")
                        .property("count", 4L)
                        .property("edgeGroup", CollectionUtil.treeSet(new String[]{"RoadHasJunction", "RoadUse" }))
                        .property("hllp", 2L)
                        .build(),
                new Entity.Builder()
                        .group("Cardinality")
                        .vertex("24")
                        .property("count", 4L)
                        .property("edgeGroup", CollectionUtil.treeSet(new String[]{"RoadHasJunction", "RoadUse" }))
                        .property("hllp", 2L)
                        .build(),
                new Entity.Builder()
                        .group("Cardinality")
                        .vertex("27")
                        .property("count", 2L)
                        .property("edgeGroup", CollectionUtil.treeSet(new String[]{"RoadHasJunction", "RoadUse" }))
                        .property("hllp", 2L)
                        .build(),
                new Entity.Builder()
                        .group("Cardinality")
                        .vertex("28")
                        .property("count", 2L)
                        .property("edgeGroup", CollectionUtil.treeSet(new String[]{"RoadHasJunction", "RoadUse" }))
                        .property("hllp", 2L)
                        .build(),
                new Entity.Builder()
                        .group("Cardinality")
                        .vertex("M5")
                        .property("count", 14L)
                        .property("edgeGroup", CollectionUtil.treeSet(new String[]{"RoadHasJunction" }))
                        .property("hllp", 7L)
                        .build()
        };

        final List<Element> results = new ArrayList<>();
        for (final Element entity : resultsItr) {
            // HyperLogLogPlus has not overridden the equals method so this is a work around to check the cardinality values are the same.
            entity.putProperty("hllp", ((HyperLogLogPlus) entity.getProperty("hllp")).cardinality());
            results.add(entity);
        }

        assertEquals(expectedResults.length, results.size());
        assertThat(results, IsCollectionContaining.hasItems(expectedResults));
    }
}