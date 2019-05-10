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

package uk.gov.gchq.gaffer.parquetstore.utils;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.collection.JavaConversions$;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.operation.handler.utilities.AggregateGafferRowsFunction;
import uk.gov.gchq.gaffer.parquetstore.testutils.DataGen;
import uk.gov.gchq.gaffer.parquetstore.testutils.TestUtils;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertThat;

public class AggregateGafferRowsFunctionTest {
    private SchemaUtils utils;

    @Before
    public void setUp() {
        final Schema schema = TestUtils.gafferSchema("schemaUsingStringVertexType");
        utils = new SchemaUtils(schema);
    }

    @After
    public void cleanUp() {
        utils = null;
    }

    @Test
    public void mergeEntityRowsTest() throws OperationException, IOException {
        final String group = "BasicEntity";
        final SchemaElementDefinition elementSchema = utils.getGafferSchema().getElement(group);
        final GafferGroupObjectConverter converter = utils.getConverter(group);
        final String[] gafferProperties = new String[elementSchema.getProperties().size()];
        elementSchema.getProperties().toArray(gafferProperties);
        final byte[] aggregatorJson = JSONSerialiser.serialise(elementSchema.getIngestAggregator());
        final AggregateGafferRowsFunction aggregator = new AggregateGafferRowsFunction(gafferProperties,
                true, elementSchema.getGroupBy(), utils.getColumnToPaths(group), aggregatorJson, converter);
        final GenericRowWithSchema row1 = DataGen.generateEntityRow(utils, group, "vertex", (byte) 'a', 0.2, 3f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), null);
        final GenericRowWithSchema row2 = DataGen.generateEntityRow(utils, group, "vertex", (byte) 'c', 0.7, 4f, TestUtils.getTreeSet2(), 7L, (short) 4, TestUtils.DATE, TestUtils.getFreqMap2(), null);
        final Row merged = aggregator.call(row1, row2);
        final List<Object> actual = new ArrayList<>(11);
        for (int i = 0; i < merged.length(); i++) {
            actual.add(merged.apply(i));
        }
        final List<Object> expected = new ArrayList<>(11);
        expected.add("vertex");
        expected.add(new byte[]{(byte) 'c'});
        expected.add(0.8999999999999999);
        expected.add(7f);
        expected.add(new String[]{"A", "B", "C"});
        expected.add(12L);
        expected.add(10);
        expected.add(TestUtils.DATE.getTime());
        expected.add(JavaConversions$.MODULE$.mapAsScalaMap(TestUtils.MERGED_FREQMAP));
        expected.add(2);
        assertThat(expected, contains(actual.toArray()));
    }

    @Test
    public void mergeEdgeRowsTest() throws OperationException, SerialisationException {
        final String group = "BasicEdge";
        final SchemaElementDefinition elementSchema = utils.getGafferSchema().getElement(group);
        final byte[] aggregatorJson = JSONSerialiser.serialise(elementSchema.getIngestAggregator());
        final GafferGroupObjectConverter converter = utils.getConverter(group);
        final String[] gafferProperties = new String[elementSchema.getProperties().size()];
        elementSchema.getProperties().toArray(gafferProperties);
        final AggregateGafferRowsFunction aggregator = new AggregateGafferRowsFunction(gafferProperties,
                false, elementSchema.getGroupBy(), utils.getColumnToPaths(group), aggregatorJson, converter);
        final GenericRowWithSchema row1 = DataGen.generateEdgeRow(utils, group, "src", "dst", true, (byte) 'a', 0.2, 3f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), null);
        final GenericRowWithSchema row2 = DataGen.generateEdgeRow(utils, group, "src", "dst", true, (byte) 'c', 0.7, 4f, TestUtils.getTreeSet2(), 7L, (short) 4, TestUtils.DATE, TestUtils.getFreqMap2(), null);
        final Row merged = aggregator.call(row1, row2);
        final List<Object> actual = new ArrayList<>(13);
        for (int i = 0; i < merged.length(); i++) {
            actual.add(merged.apply(i));
        }
        final List<Object> expected = new ArrayList<>(13);
        expected.add("src");
        expected.add("dst");
        expected.add(true);
        expected.add(new byte[]{(byte) 'c'});
        expected.add(0.8999999999999999);
        expected.add(7f);
        expected.add(new String[]{"A", "B", "C"});
        expected.add(12L);
        expected.add(10);
        expected.add(TestUtils.DATE.getTime());
        expected.add(JavaConversions$.MODULE$.mapAsScalaMap(TestUtils.MERGED_FREQMAP));
        expected.add(2);
        assertThat(expected, contains(actual.toArray()));
    }
}
