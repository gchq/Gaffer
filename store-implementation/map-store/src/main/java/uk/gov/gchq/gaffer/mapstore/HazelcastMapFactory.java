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
package uk.gov.gchq.gaffer.mapstore;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import java.util.Map;

public class HazelcastMapFactory implements MapFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(HazelcastMapFactory.class);

    private static HazelcastInstance hazelcast;

    @Override
    public void initialise(final MapStoreProperties properties) {
        if (null == hazelcast) {
            String configFile = properties.getMapFactoryConfig();
            if (configFile == null) {
                hazelcast = Hazelcast.newHazelcastInstance();
            } else {
                try {
                    final Config config = new XmlConfigBuilder(StreamUtil.openStream(getClass(), configFile)).build();
                    hazelcast = Hazelcast.newHazelcastInstance(config);
                } catch (Exception e) {
                    throw new IllegalArgumentException("Could not create cache using config path: " + configFile, e);
                }
            }
        }

        LOGGER.info("Initialised hazelcast", hazelcast.getCluster().getClusterState().name()); // bootstraps hazelcast
    }

    @Override
    public <K, V> Map<K, V> newMap(final String mapName) {
        return hazelcast.getMap(mapName);
    }

    @Override
    public void clear() {
        for (final String mapName : hazelcast.getConfig().getMapConfigs().keySet()) {
            final IMap<Object, Object> map = hazelcast.getMap(mapName);
            if (null != map) {
                map.clear();
                map.flush();
            }
        }
    }
}
