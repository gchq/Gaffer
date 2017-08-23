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

import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.exception.OverwritingException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.util.regex.Pattern;

public abstract class GraphLibrary {
    protected static final Pattern ID_ALLOWED_CHARACTERS = Pattern.compile("[a-zA-Z0-9_]*");

    public abstract void initialise(final String path);

    public void add(final String graphId, final Schema schema, final StoreProperties properties) throws OverwritingException {
        validateId(graphId);

        final byte[] schemaJson = null != schema ? schema.toJson(false) : null;

        checkExisting(graphId, schemaJson, properties);

        final String schemaId = null != schema && null != schema.getId() ? schema.getId() : graphId;
        final String propertiesId = null != properties && null != properties.getId() ? properties.getId() : graphId;

        _addIds(graphId, new Pair<>(schemaId, propertiesId));
        _addSchema(schemaId, schemaJson);
        _addProperties(propertiesId, properties);
    }

    public void addOrUpdate(final String graphId, final Schema schema, final StoreProperties properties) {
        validateId(graphId);

        final String schemaId = null != schema && null != schema.getId() ? schema.getId() : graphId;
        final String propertiesId = null != properties && null != properties.getId() ? properties.getId() : graphId;

        _addIds(graphId, new Pair<>(schemaId, propertiesId));

        if (null != schema) {
            final byte[] schemaJson = schema.toJson(false);
            _addSchema(schemaId, schemaJson);
        }

        if (null != properties) {
            _addProperties(propertiesId, properties);
        }
    }


    public Pair<Schema, StoreProperties> get(final String graphId) {
        validateId(graphId);

        final Pair<String, String> schemaAndPropsId = getIds(graphId);
        if (null == schemaAndPropsId) {
            return null;
        }

        final byte[] schemaBytes = _getSchema(schemaAndPropsId.getFirst());
        final Schema schema = null != schemaBytes ? Schema.fromJson(schemaBytes) : null;

        return new Pair<>(schema, _getProperties(schemaAndPropsId.getSecond()));
    }

    public abstract Pair<String, String> getIds(final String graphId);

    public Schema getSchema(final String schemaId) {
        validateId(schemaId);

        final byte[] schemaBytes = _getSchema(schemaId);
        return null != schemaBytes ? Schema.fromJson(schemaBytes) : null;
    }

    public StoreProperties getProperties(final String propertiesId) {
        validateId(propertiesId);

        return _getProperties(propertiesId);
    }

    public boolean exists(final String graphId) {
        return getIds(graphId) != null;
    }

    public void addSchema(final String schemaId, final Schema schema) throws OverwritingException {
        if (null != schema) {
            final byte[] schemaJson = schema.toJson(false);
            validateId(schemaId);
            _addSchema(schemaId, schemaJson);
        }
    }

    public void addProperties(final String propertiesId, final StoreProperties properties) {
        if (properties != null) {
            validateId(propertiesId);
            _addProperties(propertiesId, properties);
        }

    }

    protected abstract void _addIds(final String graphId, final Pair<String, String> schemaAndPropsIds) throws OverwritingException;

    protected abstract void _addSchema(final String schemaId, final byte[] schema) throws OverwritingException;

    protected abstract void _addProperties(final String propertiesId, final StoreProperties properties);

    protected abstract byte[] _getSchema(final String schemaId);

    protected abstract StoreProperties _getProperties(final String propertiesId);

    private void validateId(final String id) {
        if (null == id || !ID_ALLOWED_CHARACTERS.matcher(id).matches()) {
            throw new IllegalArgumentException("Id is invalid: " + id + ", it must match regex: " + ID_ALLOWED_CHARACTERS);
        }
    }

    private void checkExisting(final String graphId, final byte[] schema, final StoreProperties properties) {
        final Pair<Schema, StoreProperties> existingPair = get(graphId);
        if (null != existingPair) {
            if (null != existingPair.getFirst()) {
                if (!JsonUtil.equals(existingPair.getFirst().toJson(false), schema)) {
                    throw new OverwritingException("GraphId " + graphId + " already exists with a different schema:\n"
                            + "existing schema:\n" + StringUtil.toString(existingPair.getFirst().toJson(false))
                            + "\nnew schema:\n" + StringUtil.toString(schema));
                }
            }
            if (null != existingPair.getSecond()) {
                if (!existingPair.getSecond().equals(properties)) {
                    throw new OverwritingException("GraphId " + graphId + " already exists with a different store properties:\n"
                            + "existing storeProperties:\n" + existingPair.getSecond().toString()
                            + "\nnew storeProperties:\n" + properties.toString());
                }
            }
        }
    }
}
