/*
 * Copyright 2017. Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.utils;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class SchemaUtilsTest {

    private SchemaUtils utils;

    @Before
    public void setUp() throws StoreException {
        Logger.getRootLogger().setLevel(Level.WARN);
        final Schema schema = Schema.fromJson(getClass().getResourceAsStream("/schemaUsingStringVertexType/dataSchema.json"),
                getClass().getResourceAsStream("/schemaUsingStringVertexType/dataTypes.json"),
                getClass().getResourceAsStream("/schemaUsingStringVertexType/storeSchema.json"),
                getClass().getResourceAsStream("/schemaUsingStringVertexType/storeTypes.json"));
        final ParquetStore store = new ParquetStore(schema, new ParquetStoreProperties());
        this.utils = store.getSchemaUtils();
    }

    @After
    public void cleanUp() {
        this.utils = null;
    }

    @Test
    public void getAvroSchemaForEdgeTest() throws SerialisationException {
        final org.apache.avro.Schema schema = utils.getAvroSchema("BasicEdge");
        final String parquetSchema = "message Entity {\n" +
                "required binary " + Constants.GROUP + " (UTF8);\n" +
                "optional binary " + Constants.SOURCE + " (UTF8);\n" +
                "optional binary " + Constants.DESTINATION + " (UTF8);\n" +
                "optional boolean " + Constants.DIRECTED + " ;\n" +
                "optional binary property1 ;\n" +
                "optional double property2 ;\n" +
                "optional binary property3 ;\n" +
                "optional binary property4_raw_bytes;\n" +
                "optional int64 property4_cardinality;\n" +
                "optional int64 property5 ;\n" +
                "optional int32 property6 (INT_16);\n" +
                "optional int64 property7 ;\n" +
                "optional group property8 {\n" +
                "\toptional binary raw_bytes;\n" +
                "\toptional int64 cardinality;\n" +
                "}\n" +
                "optional int32 count ;\n" +
                "}";
        final org.apache.avro.Schema expectedSchema = new AvroSchemaConverter().convert(MessageTypeParser.parseMessageType(parquetSchema));
        assertEquals(expectedSchema, schema);
    }

    @Test
    public void getAvroSchemaForEntityTest() throws SerialisationException {
        final org.apache.avro.Schema schema = utils.getAvroSchema("BasicEntity");
        final String parquetSchema = "message Entity {\n" +
                "required binary " + Constants.GROUP + " (UTF8);\n" +
                "optional binary " + Constants.VERTEX + " (UTF8);\n" +
                "optional binary property1 ;\n" +
                "optional double property2 ;\n" +
                "optional binary property3 ;\n" +
                "optional binary property4_raw_bytes;\n" +
                "optional int64 property4_cardinality;\n" +
                "optional int64 property5 ;\n" +
                "optional int32 property6 (INT_16);\n" +
                "optional int64 property7 ;\n" +
                "optional group property8 {\n" +
                "\toptional binary raw_bytes;\n" +
                "\toptional int64 cardinality;\n" +
                "}\n" +
                "optional int32 count ;\n" +
                "}";
        final org.apache.avro.Schema expectedSchema = new AvroSchemaConverter().convert(MessageTypeParser.parseMessageType(parquetSchema));
        assertEquals(expectedSchema, schema);
    }


    @Test
    public void getColumnToSerialiserTest() throws SerialisationException {
        final HashMap<String, String> columnToSerialiser = this.utils.getColumnToSerialiser("BasicEdge");
        assertEquals("uk.gov.gchq.gaffer.parquetstore.serialisation.StringParquetSerialiser", columnToSerialiser.get(Constants.SOURCE));
        assertEquals("uk.gov.gchq.gaffer.parquetstore.serialisation.StringParquetSerialiser", columnToSerialiser.get(Constants.DESTINATION));
        assertEquals("uk.gov.gchq.gaffer.parquetstore.serialisation.BooleanParquetSerialiser", columnToSerialiser.get(Constants.DIRECTED));
        assertEquals("uk.gov.gchq.gaffer.parquetstore.serialisation.ByteParquetSerialiser", columnToSerialiser.get("property1"));
        assertEquals("uk.gov.gchq.gaffer.parquetstore.serialisation.DoubleParquetSerialiser", columnToSerialiser.get("property2"));
        assertEquals("uk.gov.gchq.gaffer.serialisation.implementation.raw.RawFloatSerialiser", columnToSerialiser.get("property3"));
        assertEquals("uk.gov.gchq.gaffer.parquetstore.serialisation.InLineHyperLogLogPlusParquetSerialiser", columnToSerialiser.get("property4"));
        assertEquals("uk.gov.gchq.gaffer.parquetstore.serialisation.LongParquetSerialiser", columnToSerialiser.get("property5"));
        assertEquals("uk.gov.gchq.gaffer.parquetstore.serialisation.ShortParquetSerialiser", columnToSerialiser.get("property6"));
        assertEquals("uk.gov.gchq.gaffer.parquetstore.serialisation.DateParquetSerialiser", columnToSerialiser.get("property7"));
        assertEquals("uk.gov.gchq.gaffer.parquetstore.serialisation.NestedHyperLogLogPlusParquetSerialiser", columnToSerialiser.get("property8"));
        assertEquals("uk.gov.gchq.gaffer.parquetstore.serialisation.IntegerParquetSerialiser", columnToSerialiser.get("count"));
    }

    @Test
    public void getEntityGroupsTest() {
        final Set<String> entityGroups = this.utils.getEntityGroups();
        final LinkedHashSet<String> expected = new LinkedHashSet<>(2);
        expected.add("BasicEntity");
        expected.add("BasicEntity2");
        assertEquals(expected, entityGroups);
    }

    @Test
    public void getEdgeGroupsTest() {
        final Set<String> edgeGroups = this.utils.getEdgeGroups();
        final LinkedHashSet<String> expected = new LinkedHashSet<>(2);
        expected.add("BasicEdge");
        expected.add("BasicEdge2");
        assertEquals(expected, edgeGroups);
    }
}
