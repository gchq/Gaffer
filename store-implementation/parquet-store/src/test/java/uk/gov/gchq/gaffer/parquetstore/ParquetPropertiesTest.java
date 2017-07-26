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

package uk.gov.gchq.gaffer.parquetstore;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ParquetPropertiesTest {
    private ParquetProperties props;

    @Before
    public void setUp() throws Exception {
        Logger.getRootLogger().setLevel(Level.WARN);
        props = new ParquetProperties();
    }

    @Test
    public void threadsAvailableTest(){
        assertEquals((Integer) 3, props.getThreadsAvailable());
        props.setThreadsAvailable(9);
        assertEquals((Integer) 9, props.getThreadsAvailable());
    }

    @Test
    public void dataDirTest(){
        assertEquals("parquet_data", props.getDataDir());
        props.setDataDir("Test");
        assertEquals("Test", props.getDataDir());
    }

    @Test
    public void tempFilesDirTest(){
        assertEquals(".gaffer/temp_parquet_data", props.getTempFilesDir());
        props.setTempFilesDir("Test");
        assertEquals("Test", props.getTempFilesDir());
    }

    @Test
    public void rowGroupSizeTest(){
        assertEquals((Integer) 4194304, props.getRowGroupSize());
        props.setRowGroupSize(100000);
        assertEquals((Integer) 100000, props.getRowGroupSize());
    }

    @Test
    public void pageSizeTest(){
        assertEquals((Integer) 1048576, props.getPageSize());
        props.setPageSize(100000);
        assertEquals((Integer) 100000, props.getPageSize());
    }

    @Test
    public void addElementsOutputFilesPerGroupTest(){
        assertEquals(100, props.getAddElementsOutputFilesPerGroup());
        props.setAddElementsOutputFilesPerGroup(10000);
        assertEquals(10000, props.getAddElementsOutputFilesPerGroup());
    }

    @Test
    public void sparkMasterTest(){
        //might fail if Spark is properly installed
        assertEquals("local[*]", props.getSparkMaster());
        props.setSparkMaster("Test");
        assertEquals("Test", props.getSparkMaster());
    }
}
