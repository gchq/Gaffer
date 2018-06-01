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

package uk.gov.gchq.gaffer.parquetstore;

import com.fasterxml.jackson.databind.Module;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiserModules;
import uk.gov.gchq.gaffer.sketches.serialisation.json.SketchesJsonModules;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class ParquetStorePropertiesTest {
    private ParquetStoreProperties props;

    @Before
    public void setUp() throws Exception {
        props = new ParquetStoreProperties();
    }

    @Test
    public void threadsAvailableTest() {
        assertEquals((Integer) 3, props.getThreadsAvailable());
        props.setThreadsAvailable(9);
        assertEquals((Integer) 9, props.getThreadsAvailable());
    }

    @Test
    public void dataDirTest() {
        assertEquals(null, props.getDataDir());
        props.setDataDir("Test");
        assertEquals("Test", props.getDataDir());
    }

    @Test
    public void tempFilesDirTest() {
        assertEquals(null, props.getTempFilesDir());
        props.setTempFilesDir("Test");
        assertEquals("Test", props.getTempFilesDir());
    }

    @Test
    public void rowGroupSizeTest() {
        assertEquals((Integer) 4194304, props.getRowGroupSize());
        props.setRowGroupSize(100000);
        assertEquals((Integer) 100000, props.getRowGroupSize());
    }

    @Test
    public void pageSizeTest() {
        assertEquals((Integer) 1048576, props.getPageSize());
        props.setPageSize(100000);
        assertEquals((Integer) 100000, props.getPageSize());
    }

    @Test
    public void sampleRateTest(){
        assertEquals((Integer) 10, props.getSampleRate());
        props.setSampleRate(100000);
        assertEquals((Integer) 100000, props.getSampleRate());
    }

    @Test
    public void addElementsOutputFilesPerGroupTest() {
        assertEquals(10, props.getAddElementsOutputFilesPerGroup());
        props.setAddElementsOutputFilesPerGroup(10000);
        assertEquals(10000, props.getAddElementsOutputFilesPerGroup());
    }

    @Test
    public void aggregateTest() {
        assertEquals(true, props.getAggregateOnIngest());
        props.setAggregateOnIngest(false);
        assertEquals(false, props.getAggregateOnIngest());
    }

    @Test
    public void sortBySplitsTest() {
        assertEquals(false, props.getSortBySplitsOnIngest());
        props.setSortBySplitsOnIngest(true);
        assertEquals(true, props.getSortBySplitsOnIngest());
    }

    @Test
    public void sparkMasterTest() {
        //might fail if Spark is properly installed
        assertEquals("local[*]", props.getSparkMaster());
        props.setSparkMaster("Test");
        assertEquals("Test", props.getSparkMaster());
    }

    @Test
    public void shouldMergeParquetJsonModules() {
        // Given
        props.setJsonSerialiserModules(TestCustomJsonModules1.class.getName() + "," + TestCustomJsonModules2.class.getName());

        // When
        final String modules = props.getJsonSerialiserModules();

        // Then
        assertEquals(SketchesJsonModules.class.getName() + "," + TestCustomJsonModules1.class.getName() + "," + TestCustomJsonModules2.class.getName(), modules);
    }

    @Test
    public void shouldMergeParquetJsonModulesAndDeduplicate() {
        // Given
        props.setJsonSerialiserModules(TestCustomJsonModules1.class.getName() + "," + SketchesJsonModules.class.getName());

        // When
        final String modules = props.getJsonSerialiserModules();

        // Then
        assertEquals(SketchesJsonModules.class.getName() + "," + TestCustomJsonModules1.class.getName(), modules);
    }

    public static final class TestCustomJsonModules1 implements JSONSerialiserModules {
        public static List<Module> modules;

        @Override
        public List<Module> getModules() {
            return modules;
        }
    }

    public static final class TestCustomJsonModules2 implements JSONSerialiserModules {
        public static List<Module> modules;

        @Override
        public List<Module> getModules() {
            return modules;
        }
    }
}
