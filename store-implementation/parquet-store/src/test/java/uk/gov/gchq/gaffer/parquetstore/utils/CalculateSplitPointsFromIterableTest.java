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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.parquetstore.operation.addelements.impl.CalculateSplitPointsFromIterable;
import uk.gov.gchq.gaffer.parquetstore.testutils.DataGen;
import uk.gov.gchq.gaffer.store.StoreException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CalculateSplitPointsFromIterableTest {

    private ExecutorService pool;

    @Before
    public void setUp() throws StoreException {
        Logger.getRootLogger().setLevel(Level.WARN);
        pool = Executors.newFixedThreadPool(1);
    }

    @Test
    public void calculateSplitsFromEmptyIterable() {
        final Iterable<Element> emptyIterable = new ArrayList<>();
        final Map<Object, Integer> splitPoints = new CalculateSplitPointsFromIterable(2, 2, emptyIterable, TestGroups.ENTITY, true).call()._2;
        Assert.assertTrue(splitPoints.isEmpty());
    }

    @Test
    public void calculateSplitsFromIterableUsingEntites() {
        final List<Element> data = new ArrayList<>();
        for (long i = 0; i < 12; i++) {
            data.add(DataGen.getEntity(TestGroups.ENTITY, i, null, null, null, null, null, null, null, null, 1, null));
            data.add(DataGen.getEntity(TestGroups.ENTITY_2, i + 5, null, null, null, null, null, null, null, null, 1, null));
        }
        final Map<Object, Integer> splitPoints = new CalculateSplitPointsFromIterable(2, 2, data, TestGroups.ENTITY, true).call()._2;
        final Map<Object, Integer> expected = new HashMap<>(2);
        expected.put(0L, 0);
        expected.put(6L, 1);
        Assert.assertEquals(expected, splitPoints);
    }

    @Test
    public void calculateSplitsFromIterableUsingEdges() {
        final List<Element> data = new ArrayList<>();
        for (long i = 0; i < 12; i++) {
            data.add(DataGen.getEdge(TestGroups.EDGE, i, i + 2, true, null, null, null, null, null, null, null, null, 1, null));
            data.add(DataGen.getEdge(TestGroups.EDGE_2, i + 5, i + 8, false, null, null, null, null, null, null, null, null, 1, null));
        }
        final Map<Object, Integer> splitPoints = new CalculateSplitPointsFromIterable(2, 2, data, TestGroups.EDGE, false).call()._2;
        final Map<Object, Integer> expected = new HashMap<>(2);
        expected.put(0L, 0);
        expected.put(6L, 1);
        Assert.assertEquals(expected, splitPoints);
    }
}
