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

package uk.gov.gchq.gaffer.data.element;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import java.util.Date;
import java.util.Map.Entry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class EdgeTest extends ElementTest {

    @Override
    @Test
    public void shouldSetAndGetFields() {
        // Given
        final Edge edge = new Edge.Builder()
                .group("group")
                .source("source vertex")
                .dest("destination vertex")
                .directed(true)
                .build();

        // When/Then
        assertEquals("group", edge.getGroup());
        assertEquals("source vertex", edge.getSource());
        assertEquals("destination vertex", edge.getDestination());
        assertTrue(edge.isDirected());
    }

    @Test
    public void shouldBuildEdge() {
        // Given
        final String source = "source vertex";
        final String destination = "dest vertex";
        final boolean directed = true;
        final String propValue = "propValue";

        // When
        final Edge edge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source(source)
                .dest(destination)
                .directed(directed)
                .property(TestPropertyNames.STRING, propValue)
                .build();

        // Then
        assertEquals(TestGroups.EDGE, edge.getGroup());
        assertEquals(source, edge.getSource());
        assertEquals(destination, edge.getDestination());
        assertTrue(edge.isDirected());
        assertEquals(propValue, edge.getProperty(TestPropertyNames.STRING));
    }

    @Test
    public void shouldConstructEdge() {
        // Given
        final String source = "source vertex";
        final String destination = "dest vertex";
        final boolean directed = true;
        final String propValue = "propValue";

        // When
        final Edge edge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source(source)
                .dest(destination)
                .directed(directed)
                .build();

        edge.putProperty(TestPropertyNames.STRING, propValue);

        // Then
        assertEquals(TestGroups.EDGE, edge.getGroup());
        assertEquals(source, edge.getSource());
        assertEquals(destination, edge.getDestination());
        assertTrue(edge.isDirected());
        assertEquals(propValue, edge.getProperty(TestPropertyNames.STRING));
    }

    @Test
    public void shouldCloneEdge() {
        // Given
        final String source = "source vertex";
        final String destination = "dest vertex";
        final boolean directed = true;

        // When
        final Edge edge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source(source)
                .dest(destination)
                .directed(directed)
                .build();

        final Edge clone = edge.emptyClone();

        // Then
        assertEquals(edge, clone);
    }

    @Override
    @Test
    public void shouldReturnTrueForEqualsWithTheSameInstance() {
        // Given
        final Edge edge = new Edge.Builder()
                .group("group")
                .source("source vertex")
                .dest("dest vertex")
                .directed(true)
                .build();

        // When
        boolean isEqual = edge.equals(edge);

        // Then
        assertTrue(isEqual);
        assertEquals(edge.hashCode(), edge.hashCode());
    }

    @Test
    public void shouldReturnTrueForShallowEqualsWhenAllCoreFieldsAreEqual() {
        // Given
        final Edge edge1 = new Edge.Builder()
                .group("group")
                .source("source vertex")
                .dest("dest vertex")
                .directed(true)
                .property("some property", "some value")
                .build();

        final Edge edge2 = cloneCoreFields(edge1);
        edge2.putProperty("some different property", "some other value");

        // When
        boolean isEqual = edge1.shallowEquals((Object) edge2);

        // Then
        assertTrue(isEqual);
    }

    @Override
    @Test
    public void shouldReturnTrueForEqualsWhenAllCoreFieldsAreEqual() {
        final Edge edge1 = new Edge.Builder()
                .group("group")
                .source("source vertex")
                .dest("dest vertex")
                .directed(true)
                .property("some property", "some value")
                .build();

        final Edge edge2 = cloneCoreFields(edge1);
        edge2.putProperty("some property", "some value");

        // When
        boolean isEqual = edge1.equals((Object) edge2);

        // Then
        assertTrue(isEqual);
        assertEquals(edge1.hashCode(), edge2.hashCode());
    }

    @Test
    public void shouldReturnFalseForEqualsWhenPropertyIsDifferent() {
        // Given
        final Edge edge1 = new Edge.Builder()
                .group("group")
                .source("source vertex")
                .dest("dest vertex")
                .directed(true)
                .property("some property", "some value")
                .build();

        final Edge edge2 = cloneCoreFields(edge1);
        edge2.putProperty("some property", "some other value");

        // When
        boolean isEqual = edge1.equals((Object) edge2);

        // Then
        assertFalse(isEqual);
        assertNotEquals(edge1.hashCode(), edge2.hashCode());
    }

    @Override
    @Test
    public void shouldReturnFalseForEqualsWhenGroupIsDifferent() {
        // Given
        final Edge edge1 = new Edge.Builder()
                .group("group")
                .source("source vertex")
                .dest("dest vertex")
                .directed(true)
                .build();

        final Edge edge2 = new Edge.Builder()
                .group("a different group")
                .source(edge1.getSource())
                .dest(edge1.getDestination())
                .directed(edge1.isDirected())
                .build();

        // When
        boolean isEqual = edge1.equals((Object) edge2);

        // Then
        assertFalse(isEqual);
        assertFalse(edge1.hashCode() == edge2.hashCode());
    }

    @Test
    public void shouldReturnFalseForEqualsWhenDirectedIsDifferent() {
        // Given
        final Edge edge1 = new Edge.Builder()
                .group("group")
                .source("source vertex")
                .dest("dest vertex")
                .directed(true)
                .build();

        final Edge edge2 = cloneCoreFields(edge1);
        edge2.setDirected(!edge1.isDirected());

        // When
        boolean isEqual = edge1.equals((Object) edge2);

        // Then
        assertFalse(isEqual);
        assertFalse(edge1.hashCode() == edge2.hashCode());
    }

    @Test
    public void shouldReturnFalseForEqualsWhenSourceIsDifferent() {
        // Given
        final Edge edge1 = new Edge.Builder()
                .group("group")
                .source("source vertex")
                .dest("dest vertex")
                .directed(true)
                .build();

        final Edge edge2 = cloneCoreFields(edge1);
        edge2.setSource("different source");

        // When
        boolean isEqual = edge1.equals((Object) edge2);

        // Then
        assertFalse(isEqual);
        assertFalse(edge1.hashCode() == edge2.hashCode());
    }

    @Test
    public void shouldReturnFalseForEqualsWhenDestinationIsDifferent() {
        // Given
        final Edge edge1 = new Edge.Builder().group("group")
                .source("source vertex")
                .dest("dest vertex")
                .directed(true)
                .build();

        final Edge edge2 = cloneCoreFields(edge1);
        edge2.setDestination("different dest vertex");

        // When
        boolean isEqual = edge1.equals((Object) edge2);

        // Then
        assertFalse(isEqual);
        assertFalse(edge1.hashCode() == edge2.hashCode());
    }

    @Test
    public void shouldReturnTrueForEqualsWhenUndirectedIdentifiersFlipped() {
        // Given
        final Edge edge1 = new Edge.Builder()
                .group("group")
                .source("source vertex")
                .dest("dest vertex")
                .directed(false)
                .build();

        // Given
        final Edge edge2 = new Edge.Builder()
                .group("group")
                .source("dest vertex")
                .dest("source vertex")
                .directed(false)
                .build();

        // When
        boolean isEqual = edge1.equals((Object) edge2);

        // Then
        assertTrue(isEqual);
        assertTrue(edge1.hashCode() == edge2.hashCode());
    }

    @Test
    public void shouldReturnFalseForEqualsWhenDirectedIdentifiersFlipped() {
        // Given
        final Edge edge1 = new Edge.Builder()
                .group("group")
                .source("source vertex")
                .dest("dest vertex")
                .directed(true)
                .build();

        // Given
        final Edge edge2 = new Edge.Builder()
                .group("group")
                .source("dest vertex")
                .dest("source vertex")
                .directed(true)
                .build();

        // When
        boolean isEqual = edge1.equals((Object) edge2);

        // Then
        assertFalse(isEqual);
        assertFalse(edge1.hashCode() == edge2.hashCode());
    }

    @Override
    @Test
    public void shouldSerialiseAndDeserialiseIdentifiers() throws SerialisationException {
        // Given
        final Edge edge = newElement("group");
        edge.setSource(1L);
        edge.setDestination(new Date(2L));
        edge.setDirected(true);

        final JSONSerialiser serialiser = new JSONSerialiser();

        // When
        final byte[] serialisedElement = serialiser.serialise(edge);
        final Edge deserialisedElement = serialiser.deserialise(serialisedElement, edge
                .getClass());

        // Then
        assertEquals(edge, deserialisedElement);
        assertTrue(StringUtil.toString(serialisedElement).contains("{\"java.lang.Long\":1}"));
        assertTrue(StringUtil.toString(serialisedElement).contains("{\"java.util.Date\":2}"));
    }

    @Override
    protected Edge newElement(final String group) {
        return new Edge.Builder().group(group).build();
    }

    @Override
    protected Edge newElement() {
        return new Edge.Builder().build();
    }

    private Edge cloneCoreFields(final Edge edge) {
        return new Edge.Builder()
                .group(edge.getGroup())
                .source(edge.getSource())
                .dest(edge.getDestination())
                .directed(edge.isDirected())
                .build();
    }

    private Edge cloneAllFields(final Edge edge) {
        final Edge newEdge = cloneCoreFields(edge);

        final Properties properties = edge.getProperties();
        for (final Entry<String, Object> entry : properties.entrySet()) {
            newEdge.putProperty(entry.getKey(), entry.getValue());
        }

        return newEdge;
    }
}
