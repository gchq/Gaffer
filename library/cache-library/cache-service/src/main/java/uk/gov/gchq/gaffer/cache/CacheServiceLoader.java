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
import uk.gov.gchq.gaffer.cache.util.CacheSystemProperty;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

/**
 * Bootstrapped when the REST service starts. Looks at a system property to determine the cache service to load.
 * Then initialises it, after which any component may use {@code CacheServiceLoader.getService()} to get the service
 * that can retrieve the appropriate cache.
 */
@WebListener(value = "Gaffer cache service loader")
public final class CacheServiceLoader implements ServletContextListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(CacheServiceLoader.class);
    private static ICacheService service;

    /**
     * Looks at a system property and initialises an appropriate cache service. If no cache service is specified in the
     * system property, the loader falls back onto a default which is backed by HashMaps.
     *
     * @throws IllegalArgumentException if an invalid cache class is specified in the system property
     */
    public static void initialise() {
        String cacheClass = System.getProperty(CacheSystemProperty.CACHE_SERVICE_CLASS);

        if (cacheClass == null) {
            LOGGER.warn("No cache class was specified. Using default with NO disk backup or distribution");
            cacheClass = CacheSystemProperty.DEFAULT_CACHE_SERVICE_CLASS;
        }
        try {
            service = Class.forName(cacheClass).asSubclass(ICacheService.class).newInstance();

        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new IllegalArgumentException("Failed to instantiate cache using class " + cacheClass, e);
        }

        service.initialise();
    }

    /**
     * @return the cache service
     */
    public static ICacheService getService() {
        if (service == null) {
            initialise();
        }
        return service;
    }

    /**
     * Triggered when the REST service starts
     *
     * @param servletContextEvent the start up event
     */
    @Override
    public void contextInitialized(final ServletContextEvent servletContextEvent) {
        initialise();
    }

    /**
     * Triggered when the REST service shuts down
     *
     * @param servletContextEvent the shut down event
     */
    @Override
    public void contextDestroyed(final ServletContextEvent servletContextEvent) {
        if (service != null) {
            service.shutdown();
        }
    }
}
