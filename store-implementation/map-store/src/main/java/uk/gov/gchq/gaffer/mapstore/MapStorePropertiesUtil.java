/*
 * Copyright 2019 Crown Copyright
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

import uk.gov.gchq.gaffer.mapstore.factory.MapFactory;
import uk.gov.gchq.gaffer.mapstore.factory.SimpleMapFactory;
import uk.gov.gchq.gaffer.store.StoreProperties;

public final class MapStorePropertiesUtil {
    public static final String CREATE_INDEX = "gaffer.store.mapstore.createIndex";
    public static final String CREATE_INDEX_DEFAULT = "true";

    public static final String MAP_FACTORY = "gaffer.store.mapstore.map.factory";
    public static final Class<? extends MapFactory> MAP_FACTORY_DEFAULT = SimpleMapFactory.class;

    public static final String MAP_FACTORY_CONFIG = "gaffer.store.mapstore.map.factory.config";
    public static final String MAP_FACTORY_CONFIG_DEFAULT = null;

    public static final String STATIC_MAP = "gaffer.store.mapstore.static";
    public static final String STATIC_MAP_DEFAULT = "false";

    /**
     * Property name for the ingest buffer size. If the value is set to less
     * than 1 then
     * no buffer is used and elements are added directly to the underlying
     * maps.
     */
    public static final String INGEST_BUFFER_SIZE = "gaffer.store.mapstore.map.ingest.buffer.size";
    public static final int INGEST_BUFFER_SIZE_DEFAULT = 0;

    private MapStorePropertiesUtil() {
        // private to prevent this class being instantiated.
        // All methods are static and should be called directly.
    }

    public static void setCreateIndex(final StoreProperties storeProperties, final boolean createIndex) {
        storeProperties.setProperty(CREATE_INDEX, Boolean.toString(createIndex));
    }

    public static boolean getCreateIndex(final StoreProperties storeProperties) {
        return Boolean.parseBoolean(storeProperties.getProperty(CREATE_INDEX, CREATE_INDEX_DEFAULT));
    }

    public static String getMapFactory(final StoreProperties storeProperties) {
        return storeProperties.getProperty(MAP_FACTORY, MAP_FACTORY_DEFAULT.getName());
    }

    public static void setMapFactory(final StoreProperties storeProperties, final String mapFactory) {
        storeProperties.setProperty(MAP_FACTORY, mapFactory);
    }

    public static void setMapFactory(final StoreProperties storeProperties, final Class<? extends MapFactory> mapFactory) {
        setMapFactory(storeProperties, mapFactory.getName());
    }

    public static String getMapFactoryConfig(final StoreProperties storeProperties) {
        return storeProperties.getProperty(MAP_FACTORY_CONFIG, MAP_FACTORY_CONFIG_DEFAULT);
    }

    public static void setMapFactoryConfig(final StoreProperties storeProperties, final String path) {
        storeProperties.setProperty(MAP_FACTORY_CONFIG, path);
    }

    public static int getIngestBufferSize(final StoreProperties storeProperties) {
        final String size = storeProperties.getProperty(INGEST_BUFFER_SIZE, null);
        if (null == size) {
            return INGEST_BUFFER_SIZE_DEFAULT;
        }

        return Integer.parseInt(size);
    }

    public static void setIngestBufferSize(final StoreProperties storeProperties, final int ingestBufferSize) {
        storeProperties.setProperty(INGEST_BUFFER_SIZE, String.valueOf(ingestBufferSize));
    }

    public static boolean isStaticMap(final StoreProperties storeProperties) {
        return Boolean.parseBoolean(storeProperties.getProperty(STATIC_MAP, STATIC_MAP_DEFAULT));
    }

    public static void setStaticMap(final StoreProperties storeProperties, final boolean staticMap) {
        storeProperties.setProperty(STATIC_MAP, Boolean.toString(staticMap));
    }
}
