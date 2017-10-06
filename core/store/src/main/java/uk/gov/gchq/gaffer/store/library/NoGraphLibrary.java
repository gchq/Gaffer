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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.exception.OverwritingException;

/**
 * A {@code NoGraphLibrary} will not store any relationships between graphIds,
 * storePropertiesIds or schemaIds.
 */
public class NoGraphLibrary extends GraphLibrary {
    private static final Logger LOGGER = LoggerFactory.getLogger(NoGraphLibrary.class);

    public NoGraphLibrary() {
        LOGGER.debug("Your schema will not be stored in a graph library. So you will need to provide it each time you create an instance of Graph.");
    }

    @Override
    public void initialise(final String path) {
        // Do nothing
    }

    @Override
    public Pair<String, String> getIds(final String graphId) {
        return null;
    }

    @Override
    protected void _addIds(final String graphId, final Pair<String, String> schemaAndPropsIds) throws OverwritingException {
        // do nothing
    }

    @Override
    protected void _addSchema(final String schemaId, final byte[] schema) throws OverwritingException {
        // do nothing
    }

    @Override
    protected void _addProperties(final String propertiesId, final StoreProperties properties) {
        // do nothing
    }

    @Override
    protected byte[] _getSchema(final String schemaId) {
        return null;
    }

    @Override
    protected StoreProperties _getProperties(final String propertiesId) {
        return null;
    }
}
