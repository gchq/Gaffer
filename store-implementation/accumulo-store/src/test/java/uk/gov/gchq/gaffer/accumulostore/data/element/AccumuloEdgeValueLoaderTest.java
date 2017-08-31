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

package uk.gov.gchq.gaffer.accumulostore.data.element;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Test;
import org.mockito.Mockito;

import uk.gov.gchq.gaffer.accumulostore.key.AccumuloElementConverter;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.LazyProperties;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;

import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class AccumuloEdgeValueLoaderTest {

    @Test
    public void shouldLoadAllIdentifiers() throws SerialisationException {
        // Given
        final String group = TestGroups.EDGE;
        final Key key = mock(Key.class);
        final Value value = mock(Value.class);
        final AccumuloElementConverter converter = mock(AccumuloElementConverter.class);
        final Schema schema = createSchema();
        final AccumuloEdgeValueLoader loader = new AccumuloEdgeValueLoader(group, key, value, converter, schema, false);
        final Edge edge = mock(Edge.class);
        final EdgeId elementId = new EdgeSeed("source", "dest", true);

        given(converter.getElementId(key, false)).willReturn(elementId);

        // When
        loader.loadIdentifiers(edge);

        // Then
        verify(edge).setIdentifiers("source", "dest", true);
        verify(converter, never()).getPropertiesFromColumnQualifier(Mockito.eq(group), Mockito.any(byte[].class));
        verify(converter, never()).getPropertiesFromColumnVisibility(Mockito.eq(group), Mockito.any(byte[].class));
        verify(converter, never()).getPropertiesFromTimestamp(Mockito.eq(group), Mockito.anyLong());
        verify(converter, never()).getPropertiesFromValue(Mockito.eq(group), Mockito.any(Value.class));
    }

    @Test
    public void shouldLoadAllColumnQualifierPropertiesWhenGetGroupByProperty() throws SerialisationException {
        // Given
        final String group = TestGroups.EDGE;
        final Key key = mock(Key.class);
        final Value value = mock(Value.class);
        final AccumuloElementConverter converter = mock(AccumuloElementConverter.class);
        final Schema schema = createSchema();
        final AccumuloEdgeValueLoader loader = new AccumuloEdgeValueLoader(group, key, value, converter, schema, false);
        final LazyProperties lazyProperties = mock(LazyProperties.class);
        final Properties properties = mock(Properties.class);
        final ByteSequence cqData = mock(ByteSequence.class);
        given(key.getColumnQualifierData()).willReturn(cqData);
        final byte[] cqBytes = {0, 1, 2, 3, 4};
        given(cqData.getBackingArray()).willReturn(cqBytes);
        given(converter.getPropertiesFromColumnQualifier(group, cqBytes)).willReturn(properties);
        given(properties.get(TestPropertyNames.PROP_1)).willReturn("propValue1");

        // When
        final Object property = loader.getProperty(TestPropertyNames.PROP_1, lazyProperties);

        // Then
        assertEquals("propValue1", property);
        verify(lazyProperties).putAll(properties);
        verify(converter, never()).getElementId(key, false);
        verify(converter, never()).getPropertiesFromColumnVisibility(Mockito.eq(group), Mockito.any(byte[].class));
        verify(converter, never()).getPropertiesFromTimestamp(Mockito.eq(group), Mockito.anyLong());
        verify(converter, never()).getPropertiesFromValue(Mockito.eq(group), Mockito.any(Value.class));
    }

    @Test
    public void shouldLoadAllValuePropertiesWhenGetProperty() throws SerialisationException {
        // Given
        final String group = TestGroups.EDGE;
        final Key key = mock(Key.class);
        final Value value = mock(Value.class);
        final AccumuloElementConverter converter = mock(AccumuloElementConverter.class);
        final Schema schema = createSchema();
        final AccumuloEdgeValueLoader loader = new AccumuloEdgeValueLoader(group, key, value, converter, schema, false);
        final LazyProperties lazyProperties = mock(LazyProperties.class);
        final Properties properties = mock(Properties.class);
        given(converter.getPropertiesFromValue(group, value)).willReturn(properties);
        given(properties.get(TestPropertyNames.PROP_3)).willReturn("propValue3");

        // When
        final Object property = loader.getProperty(TestPropertyNames.PROP_3, lazyProperties);

        // Then
        assertEquals("propValue3", property);
        verify(lazyProperties).putAll(properties);
        verify(converter, never()).getElementId(key, false);
        verify(converter, never()).getPropertiesFromColumnVisibility(Mockito.eq(group), Mockito.any(byte[].class));
        verify(converter, never()).getPropertiesFromTimestamp(Mockito.eq(group), Mockito.anyLong());
        verify(converter, never()).getPropertiesFromColumnQualifier(Mockito.eq(group), Mockito.any(byte[].class));
    }

    @Test
    public void shouldLoadAllVisibilityPropertiesWhenGetVisProperty() throws SerialisationException {
        // Given
        final String group = TestGroups.EDGE;
        final Key key = mock(Key.class);
        final Value value = mock(Value.class);
        final AccumuloElementConverter converter = mock(AccumuloElementConverter.class);
        final Schema schema = createSchema();
        final AccumuloEdgeValueLoader loader = new AccumuloEdgeValueLoader(group, key, value, converter, schema, false);
        final LazyProperties lazyProperties = mock(LazyProperties.class);
        final Properties properties = mock(Properties.class);
        final ByteSequence cvData = mock(ByteSequence.class);
        given(key.getColumnVisibilityData()).willReturn(cvData);
        final byte[] cvBytes = {0, 1, 2, 3, 4};
        given(cvData.getBackingArray()).willReturn(cvBytes);
        given(converter.getPropertiesFromColumnVisibility(group, cvBytes)).willReturn(properties);
        given(properties.get(TestPropertyNames.VISIBILITY)).willReturn("vis1");

        // When
        final Object property = loader.getProperty(TestPropertyNames.VISIBILITY, lazyProperties);

        // Then
        assertEquals("vis1", property);
        verify(lazyProperties).putAll(properties);
        verify(converter, never()).getElementId(key, false);
        verify(converter, never()).getPropertiesFromColumnQualifier(Mockito.eq(group), Mockito.any(byte[].class));
        verify(converter, never()).getPropertiesFromTimestamp(Mockito.eq(group), Mockito.anyLong());
        verify(converter, never()).getPropertiesFromValue(Mockito.eq(group), Mockito.any(Value.class));
    }

    @Test
    public void shouldLoadAllTimestampPropertiesWhenGetTimestampProperty() throws SerialisationException {
        // Given
        final String group = TestGroups.EDGE;
        final Key key = mock(Key.class);
        final Value value = mock(Value.class);
        final AccumuloElementConverter converter = mock(AccumuloElementConverter.class);
        final Schema schema = createSchema();
        final AccumuloEdgeValueLoader loader = new AccumuloEdgeValueLoader(group, key, value, converter, schema, false);
        final LazyProperties lazyProperties = mock(LazyProperties.class);
        final Properties properties = mock(Properties.class);
        final Long timestamp = 10L;
        given(key.getTimestamp()).willReturn(timestamp);
        given(converter.getPropertiesFromTimestamp(group, timestamp)).willReturn(properties);
        given(properties.get(TestPropertyNames.TIMESTAMP)).willReturn(timestamp);

        // When
        final Object property = loader.getProperty(TestPropertyNames.TIMESTAMP, lazyProperties);

        // Then
        assertEquals(timestamp, property);
        verify(lazyProperties).putAll(properties);
        verify(converter, never()).getElementId(key, false);
        verify(converter, never()).getPropertiesFromColumnQualifier(Mockito.eq(group), Mockito.any(byte[].class));
        verify(converter, never()).getPropertiesFromColumnVisibility(Mockito.eq(group), Mockito.any(byte[].class));
        verify(converter, never()).getPropertiesFromValue(Mockito.eq(group), Mockito.any(Value.class));
    }

    private Schema createSchema() {
        return new Schema.Builder()
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, "str")
                        .property(TestPropertyNames.PROP_2, "str")
                        .property(TestPropertyNames.PROP_3, "str")
                        .property(TestPropertyNames.VISIBILITY, "str")
                        .property(TestPropertyNames.TIMESTAMP, "long")
                        .groupBy(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                        .build())
                .visibilityProperty(TestPropertyNames.VISIBILITY)
                .timestampProperty(TestPropertyNames.TIMESTAMP)
                .build();
    }
}
