/*
 * Copyright 2017 Crown Copyright
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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.testutils.TestUtils;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class SchemaUtilsTest {
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
    public void getColumnToSerialiserTest() {
        final Map<String, String> columnToSerialiser = utils.getColumnToSerialiser(TestGroups.EDGE);
        assertEquals("uk.gov.gchq.gaffer.parquetstore.serialisation.impl.StringParquetSerialiser",
                columnToSerialiser.get(ParquetStore.SOURCE));
        assertEquals("uk.gov.gchq.gaffer.parquetstore.serialisation.impl.StringParquetSerialiser",
                columnToSerialiser.get(ParquetStore.DESTINATION));
        assertEquals("uk.gov.gchq.gaffer.parquetstore.serialisation.impl.BooleanParquetSerialiser",
                columnToSerialiser.get(ParquetStore.DIRECTED));
        assertEquals("uk.gov.gchq.gaffer.parquetstore.serialisation.impl.ByteParquetSerialiser",
                columnToSerialiser.get("byte"));
        assertEquals("uk.gov.gchq.gaffer.parquetstore.serialisation.impl.DoubleParquetSerialiser",
                columnToSerialiser.get("double"));
        assertEquals("uk.gov.gchq.gaffer.parquetstore.serialisation.impl.FloatParquetSerialiser",
                columnToSerialiser.get("float"));
        assertEquals("uk.gov.gchq.gaffer.parquetstore.serialisation.impl.TreeSetStringParquetSerialiser",
                columnToSerialiser.get("treeSet"));
        assertEquals("uk.gov.gchq.gaffer.parquetstore.serialisation.impl.LongParquetSerialiser",
                columnToSerialiser.get("long"));
        assertEquals("uk.gov.gchq.gaffer.parquetstore.serialisation.impl.ShortParquetSerialiser",
                columnToSerialiser.get("short"));
        assertEquals("uk.gov.gchq.gaffer.parquetstore.serialisation.impl.DateParquetSerialiser",
                columnToSerialiser.get("date"));
        assertEquals("uk.gov.gchq.gaffer.parquetstore.serialisation.impl.FreqMapParquetSerialiser",
                columnToSerialiser.get("freqMap"));
        assertEquals("uk.gov.gchq.gaffer.parquetstore.serialisation.impl.IntegerParquetSerialiser",
                columnToSerialiser.get("count"));
    }

    @Test
    public void getEntityGroupsTest() {
        final Set<String> entityGroups = utils.getEntityGroups();
        final LinkedHashSet<String> expected = new LinkedHashSet<>(2);
        expected.add(TestGroups.ENTITY);
        expected.add(TestGroups.ENTITY_2);
        assertEquals(expected, entityGroups);
    }

    @Test
    public void getEdgeGroupsTest() {
        final Set<String> edgeGroups = utils.getEdgeGroups();
        final LinkedHashSet<String> expected = new LinkedHashSet<>(2);
        expected.add(TestGroups.EDGE);
        expected.add(TestGroups.EDGE_2);
        assertEquals(expected, edgeGroups);
    }
}
