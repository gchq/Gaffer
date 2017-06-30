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

import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.util.regex.Pattern;

public abstract class SchemaLibrary {
    protected static final Pattern GRAPH_ID_ALLOWED_CHARACTERS = Pattern.compile("[a-zA-Z0-9_].*");

    public static SchemaLibrary createSchemaLibrary(final StoreProperties storeProps) {
        String schemaLibraryClassName = storeProps.getSchemaLibraryClass();
        if (null == schemaLibraryClassName) {
            schemaLibraryClassName = StoreProperties.SCHEMA_LIBRARY_CLASS_DEFAULT;
        }

        final SchemaLibrary schemaLibrary;
        try {
            schemaLibrary = Class.forName(schemaLibraryClassName).asSubclass(SchemaLibrary.class).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new RuntimeException("Unable to create schema library of type: " + schemaLibraryClassName, e);
        }
        schemaLibrary.initialise(storeProps);
        return schemaLibrary;
    }

    public static void add(final String graphId, final Schema schema, final StoreProperties storeProps) {
        createSchemaLibrary(storeProps).add(graphId, schema);
    }

    public static void addOrUpdate(final String graphId, final Schema schema, final StoreProperties storeProps) {
        createSchemaLibrary(storeProps).addOrUpdate(graphId, schema);
    }

    public abstract void initialise(final StoreProperties storeProperties);

    public void add(final String graphId, final Schema schema) throws OverwritingSchemaException {
        validateGraphId(graphId);
        final byte[] schemaJson = schema.toJson(false);
        checkExisting(graphId, schemaJson);
        _add(graphId, schemaJson);
    }

    public void addOrUpdate(final String graphId, final Schema schema) {
        validateGraphId(graphId);
        _addOrUpdate(graphId, schema.toJson(false));
    }

    public Schema get(final String graphId) {
        validateGraphId(graphId);
        final byte[] bytes = _get(graphId);
        return null != bytes ? Schema.fromJson(bytes) : null;
    }

    protected abstract void _add(final String graphId, final byte[] schema) throws OverwritingSchemaException;

    protected abstract void _addOrUpdate(final String graphId, final byte[] schema);

    protected abstract byte[] _get(final String graphId);

    protected void validateGraphId(final String graphId) {
        if (!GRAPH_ID_ALLOWED_CHARACTERS.matcher(graphId).matches()) {
            throw new IllegalArgumentException("graphId is invalid: " + graphId + ", it must match regex: " + GRAPH_ID_ALLOWED_CHARACTERS);
        }
    }

    protected void checkExisting(final String graphId, final byte[] schema) {
        final Schema existingSchema = get(graphId);
        if (null != existingSchema) {
            if (!JsonUtil.equals(existingSchema.toJson(false), schema)) {
                throw new OverwritingSchemaException("GraphId " + graphId + " already exists with a different schema:\n"
                        + "existing schema:\n" + StringUtil.toString(existingSchema.toJson(false))
                        + "\nnew schema:\n" + StringUtil.toString(schema));
            }
        }
    }

    public static class OverwritingSchemaException extends IllegalArgumentException {
        private static final long serialVersionUID = -1995995721072170558L;

        public OverwritingSchemaException() {
        }

        public OverwritingSchemaException(final String message) {
            super(message);
        }

        public OverwritingSchemaException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }
}
