/**
 * Copyright 2015 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gaffer.statistics.transform.impl;

import gaffer.statistics.SetOfStatistics;
import gaffer.statistics.impl.Count;
import gaffer.statistics.impl.MapOfCounts;
import gaffer.statistics.impl.MapOfLongCounts;
import org.junit.Test;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link StatisticsRemoverByName}. Contains tests for writing and reading, and the transform
 * method.
 */
public class TestStatisticsRemoverByName {

    @Test
    public void testWriteReadKeep() throws IOException {
        Set<String> names = new HashSet<String>();
        names.add("A");
        names.add("BBBBBB");
        StatisticsRemoverByName remover = new StatisticsRemoverByName(StatisticsRemoverByName.KeepRemove.KEEP, names);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(baos);
        remover.write(out);

        DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        StatisticsRemoverByName read = new StatisticsRemoverByName();
        read.readFields(in);

        assertEquals(remover, read);
    }

    @Test
    public void testWriteReadRemove() throws IOException {
        Set<String> names = new HashSet<String>();
        names.add("A");
        names.add("BBBBBB");
        StatisticsRemoverByName remover = new StatisticsRemoverByName(StatisticsRemoverByName.KeepRemove.REMOVE, names);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(baos);
        remover.write(out);

        DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        StatisticsRemoverByName read = new StatisticsRemoverByName();
        read.readFields(in);

        assertEquals(remover, read);
    }

    @Test
    public void testTransformWithKeep() {
        // Create SetOfStatistics
        SetOfStatistics statistics = new SetOfStatistics();
        statistics.addStatistic("A", new Count(10));
        statistics.addStatistic("B", new MapOfCounts("abc", 10));
        statistics.addStatistic("C", new MapOfLongCounts("abc", 10L));
        // Remove any statistics with names "A" or "BBBBBB"
        Set<String> names = new HashSet<String>();
        names.add("A");
        names.add("BBBBBB");
        StatisticsRemoverByName remover = new StatisticsRemoverByName(StatisticsRemoverByName.KeepRemove.KEEP, names);
        // Transform SetOfStatistics
        SetOfStatistics transformed = remover.transform(statistics);
        // Check get the right result
        SetOfStatistics expected = new SetOfStatistics();
        expected.addStatistic("A", new Count(10));
        assertEquals(expected, statistics);
    }

    @Test
    public void testTransformWithRemove() {
        // Create SetOfStatistics
        SetOfStatistics statistics = new SetOfStatistics();
        statistics.addStatistic("A", new Count(10));
        statistics.addStatistic("B", new MapOfCounts("abc", 10));
        statistics.addStatistic("C", new MapOfLongCounts("abc", 10L));
        // Remove any statistics with names "A" or "BBBBBB"
        Set<String> names = new HashSet<String>();
        names.add("A");
        names.add("BBBBBB");
        StatisticsRemoverByName remover = new StatisticsRemoverByName(StatisticsRemoverByName.KeepRemove.REMOVE, names);
        // Transform SetOfStatistics
        SetOfStatistics transformed = remover.transform(statistics);
        // Check get the right result
        SetOfStatistics expected = new SetOfStatistics();
        expected.addStatistic("B", new MapOfCounts("abc", 10));
        expected.addStatistic("C", new MapOfLongCounts("abc", 10L));
        assertEquals(expected, statistics);
    }
}
