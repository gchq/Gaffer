/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package gaffer.gafferpop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class EdgeIdTest {
    @Test
    public void shouldConstructEdgeId() {
        // Given
        final String source = "source";
        final String dest = "dest";

        // When
        final EdgeId editId = new EdgeId(source, dest);

        // Then
        assertEquals(source, editId.getSource());
        assertEquals(dest, editId.getDest());
    }

    @Test
    public void edgeIdsWithSameSourceAndDestShouldBeEqual() {
        // Given
        final String source = "source";
        final String dest = "dest";
        final EdgeId editId1 = new EdgeId(source, dest);
        final EdgeId editId2 = new EdgeId(source, dest);

        // When
        final boolean areEqual = editId1.equals(editId2);

        // Then
        assertTrue(areEqual);
        assertEquals(editId1.hashCode(), editId2.hashCode());
    }

    @Test
    public void edgeIdsWithDifferentSourceAndDestShouldNotBeEqual() {
        // Given
        final EdgeId editId1 = new EdgeId("source1", "dest1");
        final EdgeId editId2 = new EdgeId("source2", "dest2");

        // When
        final boolean areEqual = editId1.equals(editId2);

        // Then
        assertFalse(areEqual);
        assertNotEquals(editId1.hashCode(), editId2.hashCode());
    }

    @Test
    public void shouldCreateReadableToString() {
        // Given
        final EdgeId editId1 = new EdgeId("source", "dest");

        // When
        final String toString = editId1.toString();

        // Then
        assertEquals("source->dest", toString);
    }
}