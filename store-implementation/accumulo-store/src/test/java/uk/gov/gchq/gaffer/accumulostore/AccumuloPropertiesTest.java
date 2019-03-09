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

package uk.gov.gchq.gaffer.accumulostore;

import com.fasterxml.jackson.databind.Module;
import org.junit.Test;

import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiserModules;
import uk.gov.gchq.gaffer.sketches.serialisation.json.SketchesJsonModules;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StorePropertiesUtil;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AccumuloPropertiesTest {
    @Test
    public void shouldMergeAccumuloJsonModules() {
        // Given
        final StoreProperties props = new StoreProperties();
        StorePropertiesUtil.setJsonSerialiserModules(props, TestCustomJsonModules1.class.getName() + "," + TestCustomJsonModules2.class.getName());

        // When
        final String modules = AccumuloStorePropertiesUtil.getJsonSerialiserModules(props);

        // Then
        assertEquals(SketchesJsonModules.class.getName() + "," + TestCustomJsonModules1.class.getName() + "," + TestCustomJsonModules2.class.getName(), modules);
    }

    @Test
    public void shouldMergeAccumuloJsonModulesAndDeduplicate() {
        // Given
        final StoreProperties props = new StoreProperties();
        StorePropertiesUtil.setJsonSerialiserModules(props, TestCustomJsonModules1.class.getName() + "," + SketchesJsonModules.class.getName());

        // When
        final String modules = AccumuloStorePropertiesUtil.getJsonSerialiserModules(props);

        // Then
        assertEquals(SketchesJsonModules.class.getName() + "," + TestCustomJsonModules1.class.getName(), modules);
    }

    @Test
    public void shouldSetProperties() {
        // Given
        final StoreProperties props = new StoreProperties();
        final String NUM_THREADS_WRITER = "5";
        final String MAX_TIME_OUT = "500";
        final String MAX_BUFFER = "200000000";
        final String ZOOKEEPERS = "accumulo.zookeepers";
        final String INSTANCE = "accumulo.instance";
        final String NUM_THREADS_SCANNER = "8";
        final String CLIENT_SIDE_BLOOM = "786432000";
        final String FALSE_POSITIVE_RATE = "0.0003";
        final String MAX_BLOOM_FILTER = "7864320";
        final String KEY_PACKAGE_CLASS = "gaffer.store.accumulo.keypackage.class";
        final String REPLICATION_FACTOR = "accumulo.file.replication";

        // When
        AccumuloStorePropertiesUtil.setNumThreadsForBatchWriter(props, NUM_THREADS_WRITER);
        AccumuloStorePropertiesUtil.setMaxTimeOutForBatchWriterInMilliseconds(props, MAX_TIME_OUT);
        AccumuloStorePropertiesUtil.setMaxBufferSizeForBatchWriterInBytes(props, MAX_BUFFER);
        AccumuloStorePropertiesUtil.setZookeepers(props, ZOOKEEPERS);
        AccumuloStorePropertiesUtil.setInstance(props, INSTANCE);
        AccumuloStorePropertiesUtil.setThreadsForBatchScanner(props, NUM_THREADS_SCANNER);
        AccumuloStorePropertiesUtil.setClientSideBloomFilterSize(props, CLIENT_SIDE_BLOOM);
        AccumuloStorePropertiesUtil.setFalsePositiveRate(props, FALSE_POSITIVE_RATE);
        AccumuloStorePropertiesUtil.setMaxBloomFilterToPassToAnIterator(props, MAX_BLOOM_FILTER);
        AccumuloStorePropertiesUtil.setKeyPackageClass(props, KEY_PACKAGE_CLASS);
        AccumuloStorePropertiesUtil.setTableFileReplicationFactor(props, REPLICATION_FACTOR);
        AccumuloStorePropertiesUtil.setEnableValidatorIterator(props, true);

        // Then
        assertEquals(Integer.parseInt(NUM_THREADS_WRITER), AccumuloStorePropertiesUtil.getNumThreadsForBatchWriter(props));
        assertEquals(Long.parseLong(MAX_TIME_OUT), AccumuloStorePropertiesUtil.getMaxTimeOutForBatchWriterInMilliseconds(props).longValue());
        assertEquals(Long.parseLong(MAX_BUFFER), AccumuloStorePropertiesUtil.getMaxBufferSizeForBatchWriterInBytes(props).longValue());
        assertEquals(ZOOKEEPERS, AccumuloStorePropertiesUtil.getZookeepers(props));
        assertEquals(INSTANCE, AccumuloStorePropertiesUtil.getInstance(props));
        assertEquals(Integer.parseInt(NUM_THREADS_SCANNER), AccumuloStorePropertiesUtil.getThreadsForBatchScanner(props));
        assertEquals(Integer.parseInt(CLIENT_SIDE_BLOOM), AccumuloStorePropertiesUtil.getClientSideBloomFilterSize(props));
        assertEquals(Double.parseDouble(FALSE_POSITIVE_RATE), AccumuloStorePropertiesUtil.getFalsePositiveRate(props), 0.0001D);
        assertEquals(Integer.parseInt(MAX_BLOOM_FILTER), AccumuloStorePropertiesUtil.getMaxBloomFilterToPassToAnIterator(props));
        assertEquals(KEY_PACKAGE_CLASS, AccumuloStorePropertiesUtil.getKeyPackageClass(props));
        assertEquals(REPLICATION_FACTOR, AccumuloStorePropertiesUtil.getTableFileReplicationFactor(props));
        assertTrue(AccumuloStorePropertiesUtil.getEnableValidatorIterator(props));

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
