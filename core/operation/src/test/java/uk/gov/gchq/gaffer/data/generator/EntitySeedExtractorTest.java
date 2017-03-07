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

package uk.gov.gchq.gaffer.data.generator;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.data.generator.EntitySeedExtractor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class EntitySeedExtractorTest {
    @Test
    public void shouldThrowUnsupportedOperationExceptionIfGetElementCalled() {
        // Given
        final EntitySeedExtractor extractor = new EntitySeedExtractor();

        // When / Then
        try {
            extractor.getElement(mock(EntitySeed.class));
        } catch (final UnsupportedOperationException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldGetIdentifierFromEntity() {
        // Given
        final EntitySeedExtractor extractor = new EntitySeedExtractor();
        final Entity entity = new Entity(TestGroups.ENTITY, "identifier");

        // When
        final EntitySeed seed = extractor.getObject(entity);

        // Then
        assertSame("identifier", seed.getVertex());
    }

    @Test
    public void shouldGetSourceFromEdge() {
        // Given
        final EntitySeedExtractor extractor = new EntitySeedExtractor(IdentifierType.SOURCE);
        final Edge edge = new Edge(TestGroups.EDGE, "source", "destination", false);

        // When
        final EntitySeed seed = extractor.getObject(edge);

        // Then
        assertEquals("source", seed.getVertex());
    }

    @Test
    public void shouldGetDestinationFromEdge() {
        // Given
        final EntitySeedExtractor extractor = new EntitySeedExtractor(IdentifierType.DESTINATION);
        final Edge edge = new Edge(TestGroups.EDGE, "source", "destination", false);

        // When
        final EntitySeed seed = extractor.getObject(edge);

        // Then
        assertEquals("destination", seed.getVertex());
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionFromEdgeWhenIdTypeIsDirected() {
        // Given
        final EntitySeedExtractor extractor = new EntitySeedExtractor(IdentifierType.DIRECTED);
        final Edge edge = new Edge(TestGroups.EDGE, "source", "destination", false);

        // When / Then
        try {
            extractor.getObject(edge);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e);
        }
    }
}