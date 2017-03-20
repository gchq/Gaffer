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
import uk.gov.gchq.gaffer.cache.impl.HashMapCacheService;
import uk.gov.gchq.gaffer.cache.util.CacheSystemProperty;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

@WebListener(value = "Gaffer cache service loader")
public final class CacheServiceLoader implements ServletContextListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(CacheServiceLoader.class);
    private static ICacheService service;

    static {
        String cacheClass = System.getProperty(CacheSystemProperty.CACHE_SERVICE_CLASS);

        if (cacheClass == null) {
            LOGGER.warn("No cache class was specified. Using default with NO disk backup or distribution");
            cacheClass = CacheSystemProperty.DEFAULT_CACHE_SERVICE_CLASS;
        }
        try {
            service = Class.forName(cacheClass).asSubclass(ICacheService.class).newInstance();

        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            LOGGER.error("Failed to instantiate " + cacheClass, e);
            LOGGER.error("Using default cache - not distributed or indexed on disk");
            service = new HashMapCacheService();
        }
    }

    public static ICacheService getService() {
        return service;
    }

    private CacheServiceLoader() {
        // do not instantiate
    }


    @Override
    public void contextInitialized(final ServletContextEvent servletContextEvent) {
        service.initialise();
    }

    @Override
    public void contextDestroyed(final ServletContextEvent servletContextEvent) {
        service.shutdown();
    }
}
