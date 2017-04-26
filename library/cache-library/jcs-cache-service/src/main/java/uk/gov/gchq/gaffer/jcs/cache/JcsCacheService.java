/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.jcs.cache;


import org.apache.commons.io.IOUtils;
import org.apache.jcs.access.exception.CacheException;
import org.apache.jcs.engine.control.CompositeCache;
import org.apache.jcs.engine.control.CompositeCacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.cache.ICache;
import uk.gov.gchq.gaffer.cache.impl.AbstractCacheService;
import uk.gov.gchq.gaffer.cache.util.CacheSystemProperty;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class JcsCacheService extends AbstractCacheService {

    private CompositeCacheManager manager;
    private static final Logger LOGGER = LoggerFactory.getLogger(JcsCacheService.class);

    @Override
    public void initialise() {
        String configFile = System.getProperty(CacheSystemProperty.CACHE_CONFIG_FILE);
        manager = CompositeCacheManager.getUnconfiguredInstance();

        if (configFile != null) {
            try {
                Properties cacheProperties = readProperties(configFile);
                manager.configure(cacheProperties);
                return;
            } catch (IOException e) {
                throw new IllegalArgumentException("Cannot create cache using config file " + configFile, e);
            }
        }
        LOGGER.warn("No config file configured. Using default.");
        manager.configure();

    }

    private Properties readProperties(final String configFilePath) throws IOException {
        Properties props = new Properties();

        InputStream is = new FileInputStream(configFilePath);
        try {
            props.load(is);
            return props;
        } finally {
            IOUtils.closeQuietly(is);
        }
    }

    @Override
    public void shutdown() {
        manager.shutDown();
    }

    @Override
    public <K, V> ICache<K, V> getCache(final String cacheName) {
        CompositeCache cache = manager.getCache(cacheName);
        try {
            return new JcsCache<>(cache);
        } catch (CacheException e) {
            throw new IllegalArgumentException("Failed to create cache", e);
        }
    }
}
