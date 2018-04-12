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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.parquetstore.operation.handler.spark.utilities.CalculateSplitPointsFromJavaRDD;
import uk.gov.gchq.gaffer.parquetstore.testutils.DataGen;
import uk.gov.gchq.gaffer.parquetstore.testutils.TestUtils;
import uk.gov.gchq.gaffer.store.StoreException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CalculateSplitPointsFromJavaRDDTest {
    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @Before
    public void setUp() throws StoreException {
        Logger.getRootLogger().setLevel(Level.WARN);
    }

    @Test
    public void calculateSplitsFromEmptyJavaRDD() throws IOException {
        final JavaRDD<Element> emptyJavaRDD = TestUtils.getJavaSparkContext().emptyRDD();
        final Map<Object, Integer> splitPoints =
                new CalculateSplitPointsFromJavaRDD(2, 2, emptyJavaRDD, TestGroups.ENTITY, true).call()._2;
        Assert.assertTrue(splitPoints.isEmpty());
    }

    @Test
    public void calculateSplitsFromJavaRDDUsingEntities() throws IOException {
        final JavaSparkContext javaSparkContext = TestUtils.getJavaSparkContext();
        final List<Element> data = new ArrayList<>();
        for (long i = 0; i < 12; i++) {
            data.add(DataGen.getEntity(TestGroups.ENTITY, i, null, null, null, null, null, null, null, null, 1, null));
            data.add(DataGen.getEntity(TestGroups.ENTITY_2, i + 5, null, null, null, null, null, null, null, null, 1, null));
        }
        final JavaRDD<Element> dataRDD = javaSparkContext.parallelize(data);
        final Map<Object, Integer> splitPoints = new CalculateSplitPointsFromJavaRDD(1, 2, dataRDD, TestGroups.ENTITY, true).call()._2;
        final Map<Object, Integer> expected = new HashMap<>(2);
        expected.put(0L, 0);
        expected.put(6L, 1);
        Assert.assertEquals(expected, splitPoints);
    }

    @Test
    public void calculateSplitsFromJavaRDDUsingEdges() throws IOException {
        final JavaSparkContext javaSparkContext = TestUtils.getJavaSparkContext();
        final List<Element> data = new ArrayList<>();
        for (long i = 0; i < 12; i++) {
            data.add(DataGen.getEdge(TestGroups.EDGE, i, i + 2, true, null, null, null, null, null, null, null, null, 1, null));
            data.add(DataGen.getEdge(TestGroups.EDGE_2, i + 5, i + 8, false, null, null, null, null, null, null, null, null, 1, null));
        }
        final JavaRDD<Element> dataRDD = javaSparkContext.parallelize(data);
        final Map<Object, Integer> splitPoints = new CalculateSplitPointsFromJavaRDD(1, 2, dataRDD, TestGroups.EDGE, false).call()._2;
        final Map<Object, Integer> expected = new HashMap<>(2);
        expected.put(0L, 0);
        expected.put(6L, 1);
        Assert.assertEquals(expected, splitPoints);
    }
}
