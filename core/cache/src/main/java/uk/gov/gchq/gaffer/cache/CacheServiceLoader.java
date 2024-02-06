/*
 * Copyright 2016-2024 Crown Copyright
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

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Initialised when the store is initialised. Looks at a system property to determine the cache service to load.
 * Then initialises it, after which any component may use {@code CacheServiceLoader.getService()} to get the service
 * that can retrieve the appropriate cache.
 */
public final class CacheServiceLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(CacheServiceLoader.class);
    public static final String DEFAULT_SERVICE_NAME = "default";
    private static final Map<String, ICacheService> SERVICES = new HashMap<>();
    private static boolean shutdownHookAdded = false;

    /**
     * Creates a cache service identified by the given service name, using the supplied class
     * name and properties to initialise the service.
     * <p> Adds a shutdown hook which gracefully closes the cache service if JVM is stopped.
     * This shouldn't be relied upon in a servlet context, instead use ServletLifecycleListener.
     *
     * @param serviceName name to identify the cache service to initialise
     * @param cacheClass class name of the cache service provider to use
     * @param properties properties to pass to the cache service provider
     * @throws IllegalArgumentException if an invalid cache class is specified
     */
    public static void initialise(final String serviceName, final String cacheClass, final Properties properties) {
        if (isEnabled(serviceName)) {
            LOGGER.debug("Will not initialise as Cache service '{}' was already enabled.", serviceName);
            return;
        }

        if (cacheClass == null) {
            throw new IllegalArgumentException("Failed to instantiate cache, cache class was null/missing");
        } else if (serviceName == null) {
            throw new IllegalArgumentException("Failed to instantiate cache, service name was null/missing");
        }

        try {
            Class<? extends ICacheService> newCacheClass = Class.forName(cacheClass).asSubclass(ICacheService.class);
            ICacheService newCacheInstance = newCacheClass.getDeclaredConstructor().newInstance();
            SERVICES.put(serviceName, newCacheInstance);
        } catch (final InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new IllegalArgumentException("Failed to instantiate cache using class " + cacheClass, e);
        } catch (final ClassNotFoundException | ClassCastException e) {
            throw new IllegalArgumentException(String.format("Failed to instantiate cache, class '%s' is missing or invalid", cacheClass), e);
        }

        SERVICES.get(serviceName).initialise(properties);

        if (!shutdownHookAdded) {
            Runtime.getRuntime().addShutdownHook(new Thread(CacheServiceLoader::shutdown));
            shutdownHookAdded = true;
        }
    }

    /**
     * Creates a default cache service using the supplied class name to initialise the service.
     * No properties are passed to the cache service, this is primarily a convenience method.
     * See {@link #initialise(String, String, Properties)} for further details.
     *
     * @param cacheClass class name of the cache service provider to use
     * @throws IllegalArgumentException if an invalid cache class is specified
     */
    public static void initialise(final String cacheClass) {
        initialise(DEFAULT_SERVICE_NAME, cacheClass, null);
    }

    /**
     * Get the default cache service object.
     *
     * @return the default cache service
     */
    public static ICacheService getDefaultService() {
        return SERVICES.get(DEFAULT_SERVICE_NAME);
    }

    /**
     * Get the cache service identified by the supplied name.
     *
     * @param serviceName name identifying the cache service
     * @return the cache service
     */
    public static ICacheService getService(final String serviceName) {
        return SERVICES.get(serviceName);
    }

    /**
     * @return true if the default cache service is enabled
     */
    public static boolean isDefaultEnabled() {
        return SERVICES.containsKey(DEFAULT_SERVICE_NAME);
    }

    /**
     * @param serviceName name identifying a cache service
     * @return true if a cache service with that name exists (and is therefore enabled)
     */
    public static boolean isEnabled(final String serviceName) {
        return SERVICES.containsKey(serviceName);
    }

    /**
     * Looks at a system property and initialises an appropriate cache service. Adds a shutdown hook
     * which gracefully closes the cache service if JVM is stopped. This should not be relied upon
     * in a servlet context - use the ServletLifecycleListener located in the REST module instead.
     * @deprecated Instead use {@link #initialise(String, String, Properties)}, or optionally
     * {@link #initialise(String)} when writing tests.
     *
     * @param properties the cache service properties
     * @throws IllegalArgumentException if an invalid cache class is specified in the system property
     */
    @Deprecated
    public static void initialise(final Properties properties) {
        LOGGER.warn("Calling the deprecated initialise method initialises the default cache service, " +
                "this will cause problems if you are using service specific cache services");
        if (null == properties) {
            LOGGER.warn("received null properties - exiting initialise method without creating service");
            return;
        }
        final String cacheClass = (properties.getProperty(CacheProperties.CACHE_SERVICE_DEFAULT_CLASS) != null) ?
                properties.getProperty(CacheProperties.CACHE_SERVICE_DEFAULT_CLASS) :
                properties.getProperty(CacheProperties.CACHE_SERVICE_CLASS);

        if (null == cacheClass) {
            LOGGER.debug("No cache service class was specified in properties.");
            return;
        }

        if (isDefaultEnabled()) {
            LOGGER.debug("Will not initialise as Cache service was already enabled.");
            return;
        }

        try {
            Class<? extends ICacheService> newCacheClass = Class.forName(cacheClass).asSubclass(ICacheService.class);
            ICacheService newCacheInstance = newCacheClass.getDeclaredConstructor().newInstance();
            SERVICES.put(DEFAULT_SERVICE_NAME, newCacheInstance);
        } catch (final InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new IllegalArgumentException("Failed to instantiate cache using class " + cacheClass, e);
        } catch (final ClassNotFoundException | ClassCastException e) {
            throw new IllegalArgumentException(String.format("Failed to instantiate cache, class '%s' is invalid", cacheClass), e);
        }

        SERVICES.get(DEFAULT_SERVICE_NAME).initialise(properties);

        if (!shutdownHookAdded) {
            Runtime.getRuntime().addShutdownHook(new Thread(CacheServiceLoader::shutdown));
            shutdownHookAdded = true;
        }
    }

    /**
     * Get the default cache service object.
     * @deprecated Cache services should instead be
     * fetched by name using {@link #getService(String)},
     * or by using {@link #getDefaultService()}.
     *
     * @return the default cache service
     */
    @Deprecated
    public static ICacheService getService() {
        LOGGER.warn("Calling the deprecated getService method returns the default cache service, " +
                "this will cause problems if you are using service specific cache services");
        return getDefaultService();
    }

    /**
     * @return true if the default cache is enabled
     * @deprecated Use {@link #isDefaultEnabled()} instead,
     * or {@link #isEnabled(String)} with a service name.
     */
    @Deprecated
    public static boolean isEnabled() {
        LOGGER.warn("Calling the deprecated isEnabled method only returns if the default cache service " +
                "is enabled, this will cause problems if you are using service specific cache services");
        return isDefaultEnabled();
    }

    /**
     * Gracefully shutdown and reset the cache service.
     */
    public static void shutdown() {
        for (final ICacheService service: SERVICES.values()) {
            service.shutdown();
        }

        SERVICES.clear();
    }

    private CacheServiceLoader() {
        // private constructor to prevent instantiation
    }

}
