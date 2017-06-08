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

import uk.gov.gchq.gaffer.mapstore.factory.MapFactory;
import uk.gov.gchq.gaffer.mapstore.factory.SimpleMapFactory;
import uk.gov.gchq.gaffer.store.StoreProperties;
import java.io.InputStream;
import java.nio.file.Path;

public class MapStoreProperties extends StoreProperties {
    public static final String CREATE_INDEX = "gaffer.store.mapstore.createIndex";
    public static final String MAP_FACTORY = "gaffer.store.mapstore.map.factory";
    public static final String MAP_FACTORY_CONFIG = "gaffer.store.mapstore.map.factory.config";
    public static final String INGEST_BUFFER_SIZE = "gaffer.store.mapstore.map.ingest.buffer.size";

    public static final int INGEST_BUFFER_SIZE_DEFAULT = 500000;

    public MapStoreProperties() {
        super();
        set(STORE_CLASS, MapStore.class.getName());
    }

    public MapStoreProperties(final Path propFileLocation) {
        super(propFileLocation);
    }

    public static MapStoreProperties loadStoreProperties(final InputStream storePropertiesStream) {
        return (MapStoreProperties) StoreProperties.loadStoreProperties(storePropertiesStream);
    }

    @Override
    public MapStoreProperties clone() {
        return (MapStoreProperties) super.clone();
    }

    public void setCreateIndex(final String createIndex) {
        set(CREATE_INDEX, createIndex);
    }

    public boolean getCreateIndex() {
        return Boolean.parseBoolean(get(CREATE_INDEX, "true"));
    }

    public String getMapFactory() {
        return get(MAP_FACTORY, SimpleMapFactory.class.getName());
    }

    public void setMapFactory(final String mapFactory) {
        set(MAP_FACTORY, mapFactory);
    }

    public void setMapFactory(final Class<? extends MapFactory> mapFactory) {
        setMapFactory(mapFactory.getName());
    }

    public String getMapFactoryConfig() {
        return get(MAP_FACTORY_CONFIG);
    }

    public void setMapFactoryConfig(final String path) {
        set(MAP_FACTORY_CONFIG, path);
    }

    public Integer getIngestBufferSize() {
        final String size = get(INGEST_BUFFER_SIZE, null);
        if (null == size) {
            return INGEST_BUFFER_SIZE_DEFAULT;
        }

        return Integer.parseInt(size);
    }

    public void setIngestBufferSize(final int ingestBufferSize) {
        set(INGEST_BUFFER_SIZE, String.valueOf(ingestBufferSize));
    }
}
