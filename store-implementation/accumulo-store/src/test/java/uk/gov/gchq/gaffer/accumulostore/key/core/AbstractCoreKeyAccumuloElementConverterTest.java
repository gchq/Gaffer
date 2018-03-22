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
package uk.gov.gchq.gaffer.accumulostore.key.core;

import com.google.common.primitives.Bytes;
import org.junit.Test;

import uk.gov.gchq.gaffer.accumulostore.key.AbstractAccumuloElementConverterTest;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.EdgeDirection;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;

import java.io.ByteArrayOutputStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public abstract class AbstractCoreKeyAccumuloElementConverterTest extends AbstractAccumuloElementConverterTest<AbstractCoreKeyAccumuloElementConverter> {

    @Test
    public void shouldReturnOverriddenSerialiseNull() throws Exception {
        //given
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        //expected = Size in bytes + bytes of "Empty"
        byte[] expected = Bytes.concat(new byte[]{5}, "Empty".getBytes());
        Schema testSchema = new Schema.Builder()
                .edge("group1", new SchemaEdgeDefinition.Builder()
                        .source("string")
                        .destination("string")
                        .directed("true")
                        .property("prop1", "string")
                        .property("invalidProp", "unusualSerialiser")
                        .build())
                .type("string", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .serialiser(new StringSerialiser())
                        .build())
                .type("true", Boolean.class)
                .type("unusualSerialiser", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .serialiser(new UnusualTestSerialiser())
                        .build())
                .build();

        //when
        createConverter(testSchema).serialiseSizeAndPropertyValue("invalidProp", testSchema.getElement("group1"), new Properties(), stream);

        //then
        assertArrayEquals(expected, stream.toByteArray());
    }

    @Test
    public void shouldDeserialiseSourceDestinationValuesCorrectWayRound() {
        // Given 
        final Edge edge = new Edge.Builder()
                .source("1")
                .dest("2")
                .directed(true)
                .group(TestGroups.ENTITY)
                .build();

        final byte[] rowKey = converter.getRowKeysFromEdge(edge).getFirst();
        final byte[][] sourceDestValues = new byte[2][];

        // When
        final EdgeDirection direction = converter.getSourceAndDestinationFromRowKey(rowKey, sourceDestValues);

        // Then
        assertEquals(EdgeDirection.DIRECTED, direction);
    }

    @Test
    public void shouldDeserialiseSourceDestinationValuesIncorrectWayRound() {
        // Given
        final Edge edge = new Edge.Builder()
                .source("1")
                .dest("2")
                .directed(true)
                .group(TestGroups.ENTITY)
                .build();

        final byte[] rowKey = converter.getRowKeysFromEdge(edge).getSecond();
        final byte[][] sourceDestValues = new byte[2][];

        // When
        final EdgeDirection direction = converter.getSourceAndDestinationFromRowKey(rowKey, sourceDestValues);

        // Then
        assertEquals(EdgeDirection.DIRECTED_REVERSED, direction);
    }

    @Test
    public void shouldDeserialiseSourceDestinationValuesUndirected() {
        // Given 
        final Edge edge = new Edge.Builder()
                .source("1")
                .dest("2")
                .directed(false)
                .group(TestGroups.ENTITY)
                .build();

        final byte[] rowKey = converter.getRowKeysFromEdge(edge).getSecond();
        final byte[][] sourceDestValues = new byte[2][];

        // When
        final EdgeDirection direction = converter.getSourceAndDestinationFromRowKey(rowKey, sourceDestValues);

        // Then
        assertEquals(EdgeDirection.UNDIRECTED, direction);
    }

    private class UnusualTestSerialiser extends StringSerialiser {
        @Override
        public byte[] serialiseNull() {
            return "Empty".getBytes();
        }
    }
}
