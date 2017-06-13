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
package uk.gov.gchq.gaffer.accumulostore.key.core;

import com.google.common.primitives.Bytes;
import org.apache.accumulo.core.data.Key;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;

import java.io.ByteArrayOutputStream;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;

/**
 * Created on 25/05/2017.
 */
public class AbstractCoreKeyAccumuloElementConverterTest {

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
        new TestImplementation(testSchema).serialiseSizeAndPropertyValue("invalidProp", testSchema.getElement("group1"), new Properties(), stream);

        //then
        assertArrayEquals(expected, stream.toByteArray());
    }

    private class UnusualTestSerialiser extends StringSerialiser {
        @Override
        public byte[] serialiseNull() {
            return "Empty".getBytes();
        }
    }

    private class TestImplementation extends AbstractCoreKeyAccumuloElementConverter {
        public TestImplementation(Schema schema) {
            super(schema);
        }

        @Override
        protected byte[] getRowKeyFromEntity(Entity entity) {
            return new byte[0];
        }

        @Override
        protected Pair<byte[], byte[]> getRowKeysFromEdge(Edge edge) {
            return null;
        }

        @Override
        protected boolean doesKeyRepresentEntity(byte[] row) {
            return false;
        }

        @Override
        protected Entity getEntityFromKey(Key key) {
            return null;
        }

        @Override
        protected boolean getSourceAndDestinationFromRowKey(byte[] rowKey, byte[][] sourceValueDestinationValue, Map<String, String> options) {
            return false;
        }

    }
}