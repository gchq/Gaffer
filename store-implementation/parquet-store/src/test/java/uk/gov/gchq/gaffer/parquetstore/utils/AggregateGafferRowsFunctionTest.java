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

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.data.DataGen;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.RawFloatSerialiser;
import uk.gov.gchq.gaffer.store.SerialisationFactory;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaOptimiser;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class AggregateGafferRowsFunctionTest {

    private SchemaUtils utils;
    @Before
    public void setUp() throws StoreException {
        Logger.getRootLogger().setLevel(Level.WARN);
        final Schema schema = Schema.fromJson(getClass().getResourceAsStream("/schemaUsingStringVertexType/dataSchema.json"),
                getClass().getResourceAsStream("/schemaUsingStringVertexType/dataTypes.json"),
                getClass().getResourceAsStream("/schemaUsingStringVertexType/storeSchema.json"),
                getClass().getResourceAsStream("/schemaUsingStringVertexType/storeTypes.json"));
        final SchemaOptimiser optimiser = new SchemaOptimiser(new SerialisationFactory(ParquetStoreConstants.SERIALISERS));
        this.utils = new SchemaUtils(optimiser.optimise(schema, true));
    }

    @After
    public void cleanUp() {
        this.utils = null;
    }

    private HashMap<String, String> buildcolumnToAggregatorMap(final SchemaElementDefinition gafferSchema) {
        HashMap<String, String> columnToAggregatorMap = new HashMap<>();
        for (final String column : gafferSchema.getProperties()) {
            columnToAggregatorMap.put(column, gafferSchema.getPropertyTypeDef(column).getAggregateFunction().getClass().getCanonicalName());
        }
        return columnToAggregatorMap;
    }

    @Test
    public void mergeEntityRowsTest() throws OperationException, IOException {
        final String group = "BasicEntity";
        final SchemaElementDefinition elementSchema = this.utils.getGafferSchema().getElement(group);
        final HashMap<String, String> columnToAggregator = buildcolumnToAggregatorMap(elementSchema);
        final GafferGroupObjectConverter converter = this.utils.getConverter(group);
        final String[] gafferProperties = new String[elementSchema.getProperties().size()];
        elementSchema.getProperties().toArray(gafferProperties);
        final AggregateGafferRowsFunction aggregator = new AggregateGafferRowsFunction(gafferProperties,
                true, elementSchema.getGroupBy(), this.utils.getColumnToPaths(group), columnToAggregator, converter);
        final Date date = new Date();
        final HyperLogLogPlus h = new HyperLogLogPlus(5, 5);
        h.offer("A");
        h.offer("B");
        final HyperLogLogPlus h1 = new HyperLogLogPlus(5, 5);
        h.offer("A");
        h.offer("C");
        final GenericRowWithSchema row1 = DataGen.generateEntityRow(this.utils, group, "vertex", (byte) 'a', 0.2, 3f, h, 5L, (short) 6, date);
        final GenericRowWithSchema row2 = DataGen.generateEntityRow(this.utils, group, "vertex", (byte) 'c', 0.7, 4f, h1, 7L, (short) 4, date);
        final GenericRowWithSchema merged = aggregator.call(row1, row2);
        final RawFloatSerialiser floatSerialiser = new RawFloatSerialiser();
        assertEquals(12, merged.size());
        assertEquals(group, merged.apply(0));
        assertEquals("vertex", merged.apply(1));
        assertEquals((byte) 'c', ((byte[]) merged.apply(2))[0]);
        assertEquals(0.9, (double) merged.apply(3), 0.1);
        assertEquals(7f, floatSerialiser.deserialise((byte[]) merged.apply(4)), 0.1);
        assertEquals(3L, (long) merged.apply(6));
        assertEquals(12L, (long) merged.apply(7));
        assertEquals((short) 10, ((Integer) merged.apply(8)).shortValue());
        assertEquals(date, new Date((long) merged.apply(9)));
        assertEquals(3L, (long) ((GenericRow) merged.apply(10)).apply(1));
        assertEquals(2, (int) merged.apply(11));
    }

    @Test
    public void mergeEdgeRowsTest() throws OperationException, SerialisationException {
        final String group = "BasicEdge";
        final SchemaElementDefinition elementSchema = this.utils.getGafferSchema().getElement(group);
        final HashMap<String, String> columnToAggregator = buildcolumnToAggregatorMap(elementSchema);
        final GafferGroupObjectConverter converter = this.utils.getConverter(group);
        final String[] gafferProperties = new String[elementSchema.getProperties().size()];
        elementSchema.getProperties().toArray(gafferProperties);
        final AggregateGafferRowsFunction aggregator = new AggregateGafferRowsFunction(gafferProperties,
                false, elementSchema.getGroupBy(), this.utils.getColumnToPaths(group), columnToAggregator, converter);
        final Date date = new Date();
        final HyperLogLogPlus h = new HyperLogLogPlus(5, 5);
        h.offer("A");
        h.offer("B");
        final HyperLogLogPlus h1 = new HyperLogLogPlus(5, 5);
        h.offer("A");
        h.offer("C");
        final GenericRowWithSchema row1 = DataGen.generateEdgeRow(this.utils, group, "src", "dst", true, (byte) 'a', 0.2, 3f, h, 5L, (short) 6, date);
        final GenericRowWithSchema row2 = DataGen.generateEdgeRow(this.utils, group, "src", "dst", true, (byte) 'c', 0.7, 4f, h1, 7L, (short) 4, date);
        final GenericRowWithSchema merged = aggregator.call(row1, row2);
        final RawFloatSerialiser floatSerialiser = new RawFloatSerialiser();
        assertEquals(14, merged.size());
        assertEquals(group, merged.apply(0));
        assertEquals("src", merged.apply(1));
        assertEquals("dst", merged.apply(2));
        assertEquals(true, merged.apply(3));
        assertEquals((byte) 'c', ((byte[]) merged.apply(4))[0]);
        assertEquals(0.9, (double) merged.apply(5), 0.1);
        assertEquals(7f, floatSerialiser.deserialise((byte[]) merged.apply(6)), 0.1);
        assertEquals(3L, (long) merged.apply(8));
        assertEquals(12L, (long) merged.apply(9));
        assertEquals((short) 10, ((Integer) merged.apply(10)).shortValue());
        assertEquals(date, new Date((long) merged.apply(11)));
        assertEquals(3L, (long) ((GenericRow) merged.apply(12)).apply(1));
        assertEquals(2, (int) merged.apply(13));
    }
}
