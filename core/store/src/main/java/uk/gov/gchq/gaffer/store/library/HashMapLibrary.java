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

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.store.util.Config;

import java.util.HashMap;
import java.util.Map;

/**
 * A {@code HashMapLibrary} stores a {@link Library} within a HashMap.
 */
public class HashMapLibrary extends Library {

    private static final Map<String, Config> CONFIGS = new HashMap<>();

    public static void clear() {
        CONFIGS.clear();
    }

    @Override
    public void initialise(final String path) {
        // Do nothing
    }

    @Override
    public Config _getConfig(final String storeId) {
        return CONFIGS.get(storeId);
    }

    @Override
    protected void _addConfig(final String storeId, final Config config) {
        CONFIGS.put(storeId, config);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("configs", CONFIGS)
                .toString();
    }
}
