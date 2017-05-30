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
package uk.gov.gchq.gaffer.mapstore.factory;

import com.hazelcast.config.Config;
import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.mapstore.multimap.GafferToHazelcastMap;
import uk.gov.gchq.gaffer.mapstore.multimap.GafferToHazelcastMultiMap;
import uk.gov.gchq.gaffer.mapstore.multimap.MultiMap;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.io.File;
import java.io.InputStream;
import java.util.Map;

public class HazelcastMapFactory implements MapFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(HazelcastMapFactory.class);

    private static HazelcastInstance hazelcast;

    @SuppressFBWarnings(value = {"ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD", "REC_CATCH_EXCEPTION"})
    @Override
    public void initialise(final MapStoreProperties properties) {
        if (null == hazelcast) {
            String configFile = properties.getMapFactoryConfig();
            if (configFile == null) {
                hazelcast = Hazelcast.newHazelcastInstance();
            } else if (new File(configFile).exists()) {
                try {
                    final Config config = new FileSystemXmlConfig(configFile);
                    hazelcast = Hazelcast.newHazelcastInstance(config);
                } catch (Exception e) {
                    throw new IllegalArgumentException("Could not create hazelcast instance using config path: " + configFile, e);
                }
            } else {
                try (final InputStream configStream = StreamUtil.openStream(getClass(), configFile)) {
                    final Config config = new XmlConfigBuilder(configStream).build();
                    hazelcast = Hazelcast.newHazelcastInstance(config);
                } catch (final Exception e) {
                    throw new IllegalArgumentException("Could not create hazelcast instance using config resource: " + configFile, e);
                }
            }

            LOGGER.info("Initialised hazelcast: {}", hazelcast.getCluster().getClusterState().name());
        } else {
            LOGGER.debug("Hazelcast had already been initialised: {}", hazelcast.getCluster().getClusterState().name());
        }
    }

    @Override
    public <K, V> Map<K, V> getMap(final String mapName) {
        return new GafferToHazelcastMap<>(hazelcast.getMap(mapName));
    }

    @Override
    public <K, V> MultiMap<K, V> getMultiMap(final String mapName) {
        return new GafferToHazelcastMultiMap<>(hazelcast.getMultiMap(mapName));
    }

    @Override
    public <K, V> void updateValue(final Map<K, V> map, final K key, final V adaptedValue) {
        map.put(key, adaptedValue);
    }

    @Override
    public Element cloneElement(final Element element, final Schema schema) {
        // Element will already be cloned
        return element;
    }

    @Override
    public void clear() {
        for (final DistributedObject map : hazelcast.getDistributedObjects()) {
            map.destroy();
        }
    }
}
