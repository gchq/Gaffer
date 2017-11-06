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

package uk.gov.gchq.gaffer.store.library;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.commonutil.exception.OverwritingException;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.store.StoreProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * A {@code HashMapGraphLibrary} stores a {@link GraphLibrary} within three HashMaps.
 */
public class HashMapGraphLibrary extends GraphLibrary {
    private static final Map<String, Pair<String, String>> GRAPHS = new HashMap<>();
    private static final Map<String, byte[]> SCHEMAS = new HashMap<>();
    private static final Map<String, StoreProperties> PROPERTIES = new HashMap<>();

    public static void clear() {
        GRAPHS.clear();
        SCHEMAS.clear();
        PROPERTIES.clear();
    }

    @Override
    public void initialise(final String path) {
        // Do nothing
    }

    @Override
    protected void _addIds(final String graphId, final Pair<String, String> schemaAndPropsIds) throws OverwritingException {
        GRAPHS.put(graphId, schemaAndPropsIds);
    }

    @Override
    protected void _addSchema(final String schemaId, final byte[] schema) throws OverwritingException {
        SCHEMAS.put(schemaId, schema);
    }

    @Override
    protected void _addProperties(final String propertiesId, final StoreProperties properties) {
        PROPERTIES.put(propertiesId, properties.clone());
    }

    @Override
    public Pair<String, String> getIds(final String graphId) {
        return GRAPHS.get(graphId);
    }

    @Override
    protected byte[] _getSchema(final String schemaId) {
        return SCHEMAS.get(schemaId);
    }

    @Override
    protected StoreProperties _getProperties(final String propertiesId) {
        final StoreProperties storeProperties = PROPERTIES.get(propertiesId);
        return (null == storeProperties) ? null : storeProperties.clone();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("graphs", GRAPHS)
                .toString();
    }
}
