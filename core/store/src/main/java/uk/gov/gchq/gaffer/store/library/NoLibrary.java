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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.store.util.Config;

public class NoLibrary extends Library {
    private static final Logger LOGGER = LoggerFactory.getLogger(NoLibrary.class);

    @Override
    public void initialise(final String path) {
        LOGGER.debug("Your Config will not be stored in a library. So " +
                "you will need to provide it each time you create an instance" +
                " of Store.");

    }

    @Override
    protected void _addConfig(final String storeId, final Config config) {
        // Do nothing
    }

    @Override
    protected Config _getConfig(final String configId) {
        return null;
    }
}
