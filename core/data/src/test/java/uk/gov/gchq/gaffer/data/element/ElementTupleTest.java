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

package uk.gov.gchq.gaffer.data.element;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;

import static org.junit.Assert.assertEquals;

public class ElementTupleTest {
    @Test
    public void shouldGetValues() {
        // Given
        final Edge edge = new Edge.Builder()
                .group("group")
                .source("source vertex")
                .dest("destination vertex")
                .directed(true)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .property(TestPropertyNames.COUNT, 1)
                .build();

        final ElementTuple tuple = new ElementTuple(edge);

        // When / Then
        assertEquals("source vertex", tuple.get(IdentifierType.SOURCE.name()));
        assertEquals("destination vertex", tuple.get(IdentifierType.DESTINATION.name()));
        assertEquals(true, tuple.get(IdentifierType.DIRECTED.name()));
        assertEquals("source vertex", tuple.get(IdentifierType.MATCHED_VERTEX.name()));
        assertEquals("destination vertex", tuple.get(IdentifierType.ADJACENT_MATCHED_VERTEX.name()));
        assertEquals(1, tuple.get(TestPropertyNames.COUNT));
    }

    @Test
    public void shouldSetValues() {
        // Given
        final Edge edge = new Edge.Builder()
                .group("group")
                .build();

        final ElementTuple tuple = new ElementTuple(edge);

        // When
        tuple.put(IdentifierType.SOURCE.name(), "source vertex");
        tuple.put(IdentifierType.DESTINATION.name(), "destination vertex");
        tuple.put(IdentifierType.DIRECTED.name(), true);
        tuple.put(TestPropertyNames.COUNT, 1);

        // Then
        assertEquals("source vertex", edge.getSource());
        assertEquals("destination vertex", edge.getDestination());
        assertEquals(true, tuple.get(IdentifierType.DIRECTED.name()));
        assertEquals(1, tuple.get(TestPropertyNames.COUNT));

        // When
        tuple.put(IdentifierType.MATCHED_VERTEX.name(), "source vertex 2");
        tuple.put(IdentifierType.ADJACENT_MATCHED_VERTEX.name(), "destination vertex 2");

        // Then
        assertEquals("source vertex 2", tuple.get(IdentifierType.MATCHED_VERTEX.name()));
        assertEquals("destination vertex 2", tuple.get(IdentifierType.ADJACENT_MATCHED_VERTEX.name()));

    }
}
