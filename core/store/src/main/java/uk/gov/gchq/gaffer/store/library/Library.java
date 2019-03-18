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
package uk.gov.gchq.gaffer.store.library;

import uk.gov.gchq.gaffer.commonutil.exception.OverwritingException;
import uk.gov.gchq.gaffer.store.util.Config;

import java.util.regex.Pattern;

/**
 * A Library contains storeIds and their related Config.
 */
public abstract class Library {
    protected static final Pattern ID_ALLOWED_CHARACTERS = Pattern.compile("[a-zA-Z0-9_]*");
    public static final String A_LIBRARY_CAN_T_BE_ADDED_WITH_A_NULL_S_ID_S = "A Library can't be added with a null %s, Id: %s";

    public abstract void initialise(final String path);

    /**
     * Add a new relationship between a storeId and Config.
     *
     * @param storeId The StoreId to relate to.
     * @param config  The Config that relate to the graphId.
     * @throws OverwritingException If the graphId already has a related Schema and/or Config.
     */
    public void addConfig(final String storeId, final Config config) throws OverwritingException {
        validateId(storeId);
        checkExisting(storeId, config);

        nullCheck(storeId, config);

        _addConfig(storeId, config);
    }

    public void checkExisting(final String storeId,
                              final Config config) {
        final Config existingConfig = getConfig(storeId);
        if (null != existingConfig) {
            if (!existingConfig.getProperties().equals(config.getProperties())) {
                throw new OverwritingException("storeId " + storeId +
                        "already exists with a different store config:\n"
                        + "existing storeProperties:\n" + existingConfig.toString()
                        + "\nnew storeProperties:\n" + config.toString());
            }
        }
    }

    /**
     * Gets the Config given the storeId.
     *
     * @param storeId The storeId
     * @return The {@link Config} related to the storeId.
     */
    public Config getConfig(final String storeId) {
        validateId(storeId);

        return _getConfig(storeId);
    }

    public Config resolveConfig(final Config config, final String parentStoreId) {
        Config.BaseBuilder resultConfigBuilder = new Config.BaseBuilder<>();
        if (null != parentStoreId) {
            resultConfigBuilder.config(this.getConfig(parentStoreId));
        }
        if (null != config) {
            resultConfigBuilder.merge(config);
        }
        return resultConfigBuilder.build();
    }

    /**
     * Adds the config to the library against the specified storeId
     *
     * @param storeId the storeId
     * @param config  the config relating to the storeId
     */
    protected abstract void _addConfig(final String storeId, final Config config);

    protected abstract Config _getConfig(final String configId);

    private void nullCheck(final String storeId,
                           final Config config) {
        if (null == config) {
            throw new IllegalArgumentException(String.format(A_LIBRARY_CAN_T_BE_ADDED_WITH_A_NULL_S_ID_S, Config.class.getSimpleName(), storeId));
        }
    }

    private void validateId(final String id) {
        if (null == id || !ID_ALLOWED_CHARACTERS.matcher(id).matches()) {
            throw new IllegalArgumentException("Id is invalid: " + id + ", it must match regex: " + ID_ALLOWED_CHARACTERS);
        }
    }
}
