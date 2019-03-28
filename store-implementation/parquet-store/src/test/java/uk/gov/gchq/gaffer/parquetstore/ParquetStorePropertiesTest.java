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
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.junit.rules.TemporaryFolder;
import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiserModules;
import uk.gov.gchq.gaffer.sketches.serialisation.json.SketchesJsonModules;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StorePropertiesUtil;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class ParquetStorePropertiesTest {
    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    private StoreProperties props;

    @Before
    public void setUp() {
        props = new StoreProperties();
    }

    @Test
    public void threadsAvailableTest() {
        assertEquals((Integer) 3, ParquetStorePropertiesUtil.getThreadsAvailable(props));
        ParquetStorePropertiesUtil.setThreadsAvailable(props, 9);
        assertEquals((Integer) 9, ParquetStorePropertiesUtil.getThreadsAvailable(props));
    }

    @Test
    public void dataDirTest() {
        assertEquals(null, ParquetStorePropertiesUtil.getDataDir(props));
        ParquetStorePropertiesUtil.setDataDir(props, "Test");
        assertEquals("Test", ParquetStorePropertiesUtil.getDataDir(props));
    }

    @Test
    public void tempFilesDirTest() {
        assertEquals(null, ParquetStorePropertiesUtil.getTempFilesDir(props));
        ParquetStorePropertiesUtil.setTempFilesDir(props, "Test");
        assertEquals("Test", ParquetStorePropertiesUtil.getTempFilesDir(props));
    }

    @Test
    public void rowGroupSizeTest() {
        assertEquals((Integer) 4194304, ParquetStorePropertiesUtil.getRowGroupSize(props));
        ParquetStorePropertiesUtil.setRowGroupSize(props, 100000);
        assertEquals((Integer) 100000, ParquetStorePropertiesUtil.getRowGroupSize(props));
    }

    @Test
    public void pageSizeTest() {
        assertEquals((Integer) 1048576, ParquetStorePropertiesUtil.getPageSize(props));
        ParquetStorePropertiesUtil.setPageSize(props, 100000);
        assertEquals((Integer) 100000, ParquetStorePropertiesUtil.getPageSize(props));
    }

    @Test
    public void sparkMasterTest() {
        //might fail if Spark is properly installed
        assertEquals("local[*]", ParquetStorePropertiesUtil.getSparkMaster(props));
        ParquetStorePropertiesUtil.setSparkMaster(props, "Test");
        assertEquals("Test", ParquetStorePropertiesUtil.getSparkMaster(props));
    }

    @Test
    public void compressionTest() {
        assertEquals(CompressionCodecName.GZIP, ParquetStorePropertiesUtil.getCompressionCodecName(props));
        ParquetStorePropertiesUtil.setCompressionCodecName(props, CompressionCodecName.SNAPPY.name());
        assertEquals(CompressionCodecName.SNAPPY, ParquetStorePropertiesUtil.getCompressionCodecName(props));
        ParquetStorePropertiesUtil.setCompressionCodecName(props, CompressionCodecName.LZO.name());
        assertEquals(CompressionCodecName.LZO, ParquetStorePropertiesUtil.getCompressionCodecName(props));
        ParquetStorePropertiesUtil.setCompressionCodecName(props, CompressionCodecName.UNCOMPRESSED.name());
        assertEquals(CompressionCodecName.UNCOMPRESSED, ParquetStorePropertiesUtil.getCompressionCodecName(props));
    }

    @Test
    public void shouldMergeParquetJsonModules() {
        // Given
        StorePropertiesUtil.setJsonSerialiserModules(props, TestCustomJsonModules1.class.getName() + "," + TestCustomJsonModules2.class.getName());

        // When
        final String modules = ParquetStorePropertiesUtil.getJsonSerialiserModules(props);

        // Then
        assertEquals(SketchesJsonModules.class.getName() + "," + TestCustomJsonModules1.class.getName() + "," + TestCustomJsonModules2.class.getName(), modules);
    }

    @Test
    public void shouldMergeParquetJsonModulesAndDeduplicate() {
        // Given
        StorePropertiesUtil.setJsonSerialiserModules(props, TestCustomJsonModules1.class.getName() + "," + SketchesJsonModules.class.getName());

        // When
        final String modules = ParquetStorePropertiesUtil.getJsonSerialiserModules(props);

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
