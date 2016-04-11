/*
 * Copyright 2016 Crown Copyright
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
package gaffer.integration;

import gaffer.integration.AbstractStoreITs.StoreTestSuite;
import gaffer.integration.impl.GraphFunctionalityIT;
import gaffer.integration.impl.TransientPropertiesIT;
import gaffer.store.StoreProperties;
import gaffer.store.schema.Schema;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

/**
 * Runs the full suite of gaffer store integration tests. To run the tests against
 * a specific store, simply extend this class - you don't need to annotate
 * your class.
 */
@RunWith(StoreTestSuite.class)
@SuiteClasses({GraphFunctionalityIT.class, TransientPropertiesIT.class})
public abstract class AbstractStoreITs {
    private final Schema schema;
    private final StoreProperties storeProperties;

    public AbstractStoreITs(final StoreProperties storeProperties, final Schema schema) {
        this.schema = schema;
        this.storeProperties = storeProperties;
    }

    public AbstractStoreITs(final StoreProperties storeProperties) {
        this(storeProperties, new Schema());
    }

    public Schema getStoreSchema() {
        return schema;
    }

    public StoreProperties getStoreProperties() {
        return storeProperties;
    }

    public static class StoreTestSuite extends Suite {
        public StoreTestSuite(final Class<?> clazz, final RunnerBuilder builder) throws InitializationError, IllegalAccessException, InstantiationException {
            super(clazz, builder);

            final AbstractStoreITs runner = clazz.asSubclass(AbstractStoreITs.class).newInstance();
            Schema storeSchema = runner.getStoreSchema();
            if (null == storeSchema) {
                storeSchema = new Schema();
            }

            AbstractStoreIT.setStoreSchema(storeSchema);
            AbstractStoreIT.setStoreProperties(runner.getStoreProperties());
        }
    }
}
