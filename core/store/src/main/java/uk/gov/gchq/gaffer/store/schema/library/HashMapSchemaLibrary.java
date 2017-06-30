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
package uk.gov.gchq.gaffer.store.schema.library;

import uk.gov.gchq.gaffer.store.StoreProperties;
import java.util.HashMap;
import java.util.Map;

public class HashMapSchemaLibrary extends SchemaLibrary {
    private static final Map<String, byte[]> SCHEMAS = new HashMap<>();

    @Override
    public void initialise(final StoreProperties storeProperties) {
    }

    @Override
    protected void _add(final String graphId, final byte[] schema) {
        SCHEMAS.put(graphId, schema);
    }

    @Override
    protected void _addOrUpdate(final String graphId, final byte[] schema) {
        _add(graphId, schema);
    }

    @Override
    protected byte[] _get(final String graphId) {
        return SCHEMAS.get(graphId);
    }
}
