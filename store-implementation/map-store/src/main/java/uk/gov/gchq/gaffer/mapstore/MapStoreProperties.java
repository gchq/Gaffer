/*
 * Copyright 2017-2018 Crown Copyright
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

import java.io.InputStream;
import java.nio.file.Path;

/**
 * Additional {@link StoreProperties} for the {@link MapStore}.
 */
public class MapStoreProperties extends StoreProperties {
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

    public MapStoreProperties() {
        super(MapStore.class);
    }

    public MapStoreProperties(final Path propFileLocation) {
        super(propFileLocation, MapStore.class);
    }

    public static MapStoreProperties loadStoreProperties(final String pathStr) {
        return StoreProperties.loadStoreProperties(pathStr, MapStoreProperties.class);
    }

    public static MapStoreProperties loadStoreProperties(final InputStream storePropertiesStream) {
        return StoreProperties.loadStoreProperties(storePropertiesStream, MapStoreProperties.class);
    }

    public static MapStoreProperties loadStoreProperties(final Path storePropertiesPath) {
        return StoreProperties.loadStoreProperties(storePropertiesPath, MapStoreProperties.class);
    }

    @Override
    public MapStoreProperties clone() {
        return (MapStoreProperties) super.clone();
    }

    public void setCreateIndex(final boolean createIndex) {
        set(CREATE_INDEX, Boolean.toString(createIndex));
    }

    public boolean getCreateIndex() {
        return Boolean.parseBoolean(get(CREATE_INDEX, CREATE_INDEX_DEFAULT));
    }

    public String getMapFactory() {
        return get(MAP_FACTORY, MAP_FACTORY_DEFAULT.getName());
    }

    public void setMapFactory(final String mapFactory) {
        set(MAP_FACTORY, mapFactory);
    }

    public void setMapFactory(final Class<? extends MapFactory> mapFactory) {
        setMapFactory(mapFactory.getName());
    }

    public String getMapFactoryConfig() {
        return get(MAP_FACTORY_CONFIG, MAP_FACTORY_CONFIG_DEFAULT);
    }

    public void setMapFactoryConfig(final String path) {
        set(MAP_FACTORY_CONFIG, path);
    }

    public int getIngestBufferSize() {
        final String size = get(INGEST_BUFFER_SIZE, null);
        if (null == size) {
            return INGEST_BUFFER_SIZE_DEFAULT;
        }

        return Integer.parseInt(size);
    }

    public void setIngestBufferSize(final int ingestBufferSize) {
        set(INGEST_BUFFER_SIZE, String.valueOf(ingestBufferSize));
    }

    public boolean isStaticMap() {
        return Boolean.parseBoolean(get(STATIC_MAP, STATIC_MAP_DEFAULT));
    }

    public void setStaticMap(final boolean staticMap) {
        set(STATIC_MAP, Boolean.toString(staticMap));
    }
}
