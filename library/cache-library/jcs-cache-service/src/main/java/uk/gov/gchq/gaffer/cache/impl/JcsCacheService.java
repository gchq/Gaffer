/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.cache.impl;

import org.apache.commons.io.IOUtils;
import org.apache.commons.jcs.access.exception.CacheException;
import org.apache.commons.jcs.engine.control.CompositeCache;
import org.apache.commons.jcs.engine.control.CompositeCacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.cache.ICache;
import uk.gov.gchq.gaffer.cache.ICacheService;
import uk.gov.gchq.gaffer.cache.util.CacheProperties;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Implementation of the {@link ICacheService} interface which uses a {@link JcsCache}
 * as the cache implementation.
 */
public class JcsCacheService implements ICacheService {
    private static final Logger LOGGER = LoggerFactory.getLogger(JcsCacheService.class);
    private CompositeCacheManager manager;

    @Override
    public void initialise(final Properties properties) {
        String configFile = properties.getProperty(CacheProperties.CACHE_CONFIG_FILE);
        manager = CompositeCacheManager.getUnconfiguredInstance();

        if (null != configFile) {
            try {
                Properties cacheProperties = readProperties(configFile);
                manager.configure(cacheProperties);
                return;
            } catch (final IOException e) {
                throw new IllegalArgumentException("Cannot create cache using config file " + configFile, e);
            }
        }
        LOGGER.debug("No config file configured. Using default.");
        manager.configure();
    }

    @Override
    public <K, V> ICache<K, V> getCache(final String cacheName) {
        CompositeCache cache = manager.getCache(cacheName);
        try {
            return new JcsCache<>(cache);
        } catch (final CacheException e) {
            throw new IllegalArgumentException("Failed to create cache", e);
        }
    }

    @Override
    public void shutdown() {
        if (manager.isInitialized()) {
            LOGGER.debug("Shutting down JCS cache service...");
            manager.shutDown();
        }
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
}
