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

import uk.gov.gchq.gaffer.store.StoreProperties;

import java.io.InputStream;
import java.nio.file.Path;

/**
 *
 */
public class MapStoreProperties extends StoreProperties {
    public static final String MAP_CLASS = "gaffer.store.mapstore.map.class";
    public static final String CREATE_INDEX = "gaffer.store.mapstore.createIndex";

    public MapStoreProperties() {
        super();
        set(STORE_CLASS, MapStore.class.getName());
    }

    public MapStoreProperties(final Path propFileLocation) {
        super(propFileLocation);
    }

    public static MapStoreProperties loadStoreProperties(final InputStream storePropertiesStream) {
        return ((MapStoreProperties) StoreProperties.loadStoreProperties(storePropertiesStream));
    }

    @Override
    public MapStoreProperties clone() {
        return (MapStoreProperties) super.clone();
    }

    public void setMapClass(final String mapClass) {
        set(MAP_CLASS, mapClass);
    }

    public String getMapClass() {
        return get(MAP_CLASS, "java.util.HashMap");
    }

    public void setCreateIndex(final String createIndex) {
        set(CREATE_INDEX, createIndex);
    }

    public boolean getCreateIndex() {
        return Boolean.parseBoolean(get(CREATE_INDEX, "true"));
    }

}
