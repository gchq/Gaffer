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

package uk.gov.gchq.gaffer.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.cache.util.CacheProperties;
import java.util.Properties;

/**
 * Initialised when the store is initialised. Looks at a system property to determine the uk.gov.gchq.gaffer.cache service to load.
 * Then initialises it, after which any component may use {@code CacheServiceLoader.getService()} to get the service
 * that can retrieve the appropriate uk.gov.gchq.gaffer.cache.
 */
public final class CacheServiceLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(CacheServiceLoader.class);
    private static ICacheService service;

    /**
     * Looks at a system property and initialises an appropriate uk.gov.gchq.gaffer.cache service. If no uk.gov.gchq.gaffer.cache service is specified in the
     * system property, the loader falls back onto a default which is backed by HashMaps.
     *
     * @param properties the cache service properties
     * @throws IllegalArgumentException if an invalid uk.gov.gchq.gaffer.cache class is specified in the system property
     */
    public static void initialise(final Properties properties) {
        if (properties == null) {
            LOGGER.warn("received null properties - exiting initialise method without creating service");
            return;
        }
        String cacheClass = properties.getProperty(CacheProperties.CACHE_SERVICE_CLASS);

        if (cacheClass == null) {
            if (service == null) {
                LOGGER.debug("No cache service class was specified in properties.");
            }
            return;
        }
        try {
            service = Class.forName(cacheClass).asSubclass(ICacheService.class).newInstance();

        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new IllegalArgumentException("Failed to instantiate cache using class " + cacheClass, e);
        }

        service.initialise(properties);
    }

    /**
     * @return the uk.gov.gchq.gaffer.cache service
     */
    public static ICacheService getService() {
        return service;
    }

    public static void shutdown() {
        if (service != null) {
            service.shutdown();
        }
    }

    private CacheServiceLoader() {
        // do not instantiate
    }

}
