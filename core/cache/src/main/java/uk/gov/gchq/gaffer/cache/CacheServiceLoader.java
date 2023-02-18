/*
 * Copyright 2016-2023 Crown Copyright
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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.cache.util.CacheProperties;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

/**
 * Initialised when the store is initialised. Looks at a system property to determine the cache service to load.
 * Then initialises it, after which any component may use {@code CacheServiceLoader.getService()} to get the service
 * that can retrieve the appropriate cache.
 */
public final class CacheServiceLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(CacheServiceLoader.class);

    private static HashMap<String, ICacheService> services;

    private static List<String> shutdownHooks;

    /**
     * Looks at a system property and initialises an appropriate cache service. Adds a shutdown hook
     * which gracefully closes the cache service if JVM is stopped. This should not be relied upon
     * in a servlet context - use the ServletLifecycleListener located in the REST module instead
     *
     * @param properties the cache service properties
     * @return the created cache service
     * @throws IllegalArgumentException if an invalid cache class is specified in the system property
     */
    public static ICacheService initialise(final Properties properties) {
        if (null == properties) {
            LOGGER.warn("received null properties - exiting initialise method without creating service");
            return null;
        }
        final String cacheClass = properties.getProperty(CacheProperties.CACHE_SERVICE_CLASS);

        if (null == cacheClass) {
            LOGGER.debug("No cache service class was specified in properties.");
            return null;
        }

        final ICacheService service;
        try {
            service = Class.forName(cacheClass).asSubclass(ICacheService.class).newInstance();

        } catch (final InstantiationException | IllegalAccessException |
                       ClassNotFoundException e) {
            throw new IllegalArgumentException("Failed to instantiate cache using class " + cacheClass, e);
        }

        service.initialise(properties);

        if (isNull(services)) {
            services = new HashMap<>();
        }

        if (services.containsKey(cacheClass)) {
            LOGGER.debug(String.format("Trying to overwrite an existing Cache service: %s cache should be shut down first", cacheClass));
        } else {
            LOGGER.info(String.format("Cache added to services: %s", cacheClass));
            services.put(cacheClass, service);
        }

        if (isNull(shutdownHooks)) {
            shutdownHooks = new ArrayList<>();
        }

        if (!shutdownHooks.contains(cacheClass)) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> CacheServiceLoader.shutdown(cacheClass)));
            shutdownHooks.add(cacheClass);
        }

        return service;
    }

    /**
     * Get the cache service object.
     *
     * @param cacheClass the cache class to use.
     * @return the cache service
     */
    @SuppressFBWarnings(value = "MS_EXPOSE_REP", justification = "Intended behaviour")
    public static ICacheService getService(final String cacheClass) {
        return nonNull(cacheClass) ? services.get(cacheClass) : null;
    }

    /**
     * @return true if the cache is enabled
     */
    public static boolean isEnabled() {
        return nonNull(services) && !services.isEmpty();
    }

    /**
     * Gracefully shutdown and reset the cache service.
     */
    public static void shutdownAll() {
        if (nonNull(services)) {
            services.forEach((k, service) -> service.shutdown());
            services.clear();
        }
    }

    public static void shutdown(final String cacheClass) {
        if (nonNull(services) && services.containsKey(cacheClass)) {
            services.remove(cacheClass).shutdown();
            LOGGER.debug(String.format("Cache service removed %s", cacheClass));
        }
    }

    private CacheServiceLoader() {
        // private constructor to prevent instantiation
    }

}
