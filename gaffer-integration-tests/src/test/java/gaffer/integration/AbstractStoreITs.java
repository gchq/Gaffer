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
import gaffer.store.StoreProperties;
import gaffer.store.schema.Schema;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Runs the full suite of gaffer store integration tests. To run the tests against
 * a specific store, simply extend this class - you don't need to annotate
 * your class.
 */
@RunWith(StoreTestSuite.class)
public abstract class AbstractStoreITs {
    private final StoreProperties storeProperties;
    private final Schema schema;
    private final Collection<? extends Class<? extends AbstractStoreIT>> extraTests;
    private final Map<Class<? extends AbstractStoreIT>, String> skipTests = new HashMap<>();

    public AbstractStoreITs(final StoreProperties storeProperties, final Schema schema, final Collection<? extends Class<? extends AbstractStoreIT>> extraTests) {
        this.schema = schema;
        this.storeProperties = storeProperties;
        this.extraTests = extraTests;
    }

    public AbstractStoreITs(final StoreProperties storeProperties, final Collection<? extends Class<? extends AbstractStoreIT>> extraTests) {
        this(storeProperties, new Schema(), extraTests);
    }

    public AbstractStoreITs(final StoreProperties storeProperties, final Schema schema) {
        this(storeProperties, schema, Collections.<Class<? extends AbstractStoreIT>>emptyList());
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

    public Collection<? extends Class<? extends AbstractStoreIT>> getExtraTests() {
        return extraTests;
    }

    public Map<? extends Class<? extends AbstractStoreIT>, String> getSkipTests() {
        return skipTests;
    }

    protected void skipTest(final Class<? extends AbstractStoreIT> testClass, final String justification) {
        skipTests.put(testClass, justification);
    }

    public static class StoreTestSuite extends Suite {
        private static final Logger LOGGER = LoggerFactory.getLogger(StoreTestSuite.class);

        public StoreTestSuite(final Class<?> clazz, final RunnerBuilder builder) throws InitializationError, IllegalAccessException, InstantiationException {
            super(builder, clazz, getTestClasses(clazz));

            final AbstractStoreITs runner = clazz.asSubclass(AbstractStoreITs.class).newInstance();
            Schema storeSchema = runner.getStoreSchema();
            if (null == storeSchema) {
                storeSchema = new Schema();
            }

            AbstractStoreIT.setStoreSchema(storeSchema);
            AbstractStoreIT.setStoreProperties(runner.getStoreProperties());
            AbstractStoreIT.setSkipTests(runner.getSkipTests());
        }

        private static Class[] getTestClasses(final Class<?> clazz) throws IllegalAccessException, InstantiationException {
            final Set<Class<? extends AbstractStoreIT>> classes = new Reflections(AbstractStoreIT.class.getPackage().getName()).getSubTypesOf(AbstractStoreIT.class);
            keepPublicConcreteClasses(classes);

            final AbstractStoreITs runner = clazz.asSubclass(AbstractStoreITs.class).newInstance();
            classes.addAll(runner.getExtraTests());

            return classes.toArray(new Class[classes.size()]);
        }

        private static void keepPublicConcreteClasses(final Set<Class<? extends AbstractStoreIT>> classes) {
            if (null != classes) {
                final Iterator<Class<? extends AbstractStoreIT>> itr = classes.iterator();
                for (Class clazz = null; itr.hasNext(); clazz = itr.next()) {
                    if (null != clazz) {
                        final int modifiers = clazz.getModifiers();
                        if (Modifier.isAbstract(modifiers) || Modifier.isInterface(modifiers) || Modifier.isPrivate(modifiers) || Modifier.isProtected(modifiers)) {
                            itr.remove();
                        }
                    }
                }
            }
        }
    }
}
