/*
 * Copyright 2017-2018. Crown Copyright
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.parquetstore.index.ColumnIndex;
import uk.gov.gchq.gaffer.parquetstore.index.GraphIndex;
import uk.gov.gchq.gaffer.parquetstore.index.GroupIndex;
import uk.gov.gchq.gaffer.parquetstore.index.MinValuesWithPath;
import uk.gov.gchq.gaffer.parquetstore.operation.handler.utilities.CalculateSplitPointsFromIndex;
import uk.gov.gchq.gaffer.parquetstore.testutils.TestUtils;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CalculateSplitPointsFromIndexTest {
    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    private SchemaUtils schemaUtils;
    private Schema gafferSchema;
    private ExecutorService pool;

    @Before
    public void setUp() throws StoreException {
        Logger.getRootLogger().setLevel(Level.WARN);
        gafferSchema = TestUtils.gafferSchema("schemaUsingLongVertexType");
        schemaUtils = new SchemaUtils(gafferSchema);
        pool = Executors.newFixedThreadPool(1);
    }

    @Test
    public void calculateSplitsFromEmptyIndex() throws IOException, OperationException, SerialisationException {
        final ParquetStoreProperties properties = TestUtils.getParquetStoreProperties(testFolder);
        final Iterable<Element> emptyIterable = new ArrayList<>();
        final GraphIndex emptyIndex = new GraphIndex();
        final Map<String, Map<Object, Integer>> splitPoints = CalculateSplitPointsFromIndex
                .apply(emptyIndex, schemaUtils, properties, emptyIterable, pool);
        for (final String group : gafferSchema.getGroups()) {
            Assert.assertFalse(splitPoints.containsKey(group));
        }
    }

    @Test
    public void calculateSplitsFromIndexUsingEntities() throws IOException, OperationException, SerialisationException, StoreException {
        final ParquetStoreProperties properties = TestUtils.getParquetStoreProperties(testFolder);
        final Iterable<Element> emptyIterable = new ArrayList<>();
        final GraphIndex index = new GraphIndex();
        final GroupIndex entityGroupIndex = new GroupIndex();
        index.add(TestGroups.ENTITY, entityGroupIndex);
        final ColumnIndex vrtIndex = new ColumnIndex();
        entityGroupIndex.add(ParquetStoreConstants.VERTEX, vrtIndex);
        vrtIndex.add(new MinValuesWithPath(new Object[]{0L}, "part-00000.parquet"));
        vrtIndex.add(new MinValuesWithPath(new Object[]{6L}, "part-00001.parquet"));
        final Map<String, Map<Object, Integer>> splitPoints = CalculateSplitPointsFromIndex
                .apply(index, schemaUtils, properties, emptyIterable, pool);
        final Map<Object, Integer> expected = new HashMap<>(2);
        expected.put(0L, 0);
        expected.put(6L, 1);
        for (final String group : gafferSchema.getGroups()) {
            if (TestGroups.ENTITY.equals(group)) {
                Assert.assertEquals(expected, splitPoints.get(TestGroups.ENTITY));
            } else {
                Assert.assertFalse(splitPoints.containsKey(group));
            }
        }
    }

    @Test
    public void calculateSplitsFromIndexUsingEdges() throws IOException, OperationException, SerialisationException, StoreException {
        final ParquetStoreProperties properties = TestUtils.getParquetStoreProperties(testFolder);
        final Iterable<Element> emptyIterable = new ArrayList<>();
        final GraphIndex index = new GraphIndex();
        final GroupIndex entityGroupIndex = new GroupIndex();
        index.add(TestGroups.EDGE, entityGroupIndex);
        final ColumnIndex srcIndex = new ColumnIndex();
        entityGroupIndex.add(ParquetStoreConstants.SOURCE, srcIndex);
        srcIndex.add(new MinValuesWithPath(new Object[]{0L}, "part-00000.parquet"));
        srcIndex.add(new MinValuesWithPath(new Object[]{6L}, "part-00001.parquet"));
        final Map<String, Map<Object, Integer>> splitPoints = CalculateSplitPointsFromIndex
                .apply(index, schemaUtils, properties, emptyIterable, pool);
        final Map<Object, Integer> expected = new HashMap<>(2);
        expected.put(0L, 0);
        expected.put(6L, 1);
        for (final String group : gafferSchema.getGroups()) {
            if (TestGroups.EDGE.equals(group)) {
                Assert.assertEquals(expected, splitPoints.get(TestGroups.EDGE));
            } else {
                Assert.assertFalse(splitPoints.containsKey(group));
            }
        }
    }
}
