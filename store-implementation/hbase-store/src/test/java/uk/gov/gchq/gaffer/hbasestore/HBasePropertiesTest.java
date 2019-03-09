/*
 * Copyright 2016-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.hbasestore;

import com.fasterxml.jackson.databind.Module;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiserModules;
import uk.gov.gchq.gaffer.sketches.serialisation.json.SketchesJsonModules;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StorePropertiesUtil;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class HBasePropertiesTest {
    @Test
    public void shouldGetAndSetProperties() throws StoreException, IOException {
        // Given
        final StoreProperties properties = new StoreProperties();

        // When
        HBaseStorePropertiesUtil.setDependencyJarsHdfsDirPath(properties, "pathTo/jars");
        HBaseStorePropertiesUtil.setWriteBufferSize(properties, 10);
        HBaseStorePropertiesUtil.setZookeepers(properties, "zookeeper1,zookeeper2");

        // Then
        assertEquals(new Path("pathTo/jars"), HBaseStorePropertiesUtil.getDependencyJarsHdfsDirPath(properties));
        assertEquals(10, HBaseStorePropertiesUtil.getWriteBufferSize(properties));
        assertEquals("zookeeper1,zookeeper2", HBaseStorePropertiesUtil.getZookeepers(properties));
    }

    @Test
    public void shouldMergeHBaseJsonModules() {
        // Given
        final StoreProperties props = new StoreProperties();
        StorePropertiesUtil.setJsonSerialiserModules(props, TestCustomJsonModules1.class.getName() + "," + TestCustomJsonModules2.class.getName());

        // When
        final String modules = HBaseStorePropertiesUtil.getJsonSerialiserModules(props);

        // Then
        assertEquals(SketchesJsonModules.class.getName() + "," + TestCustomJsonModules1.class.getName() + "," + TestCustomJsonModules2.class.getName(), modules);
    }

    @Test
    public void shouldMergeHBaseJsonModulesAndDeduplicate() {
        // Given
        final StoreProperties props = new StoreProperties();
        StorePropertiesUtil.setJsonSerialiserModules(props, TestCustomJsonModules1.class.getName() + "," + SketchesJsonModules.class.getName());

        // When
        final String modules = HBaseStorePropertiesUtil.getJsonSerialiserModules(props);

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
