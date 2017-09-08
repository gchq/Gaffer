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
package uk.gov.gchq.gaffer.integration;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;
import org.reflections.Reflections;

import uk.gov.gchq.gaffer.integration.AbstractStoreITs.StoreTestSuite;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Runs the full suite of gaffer store integration tests. To run the tests against
 * a specific store, simply extend this class - you don't need to annotate
 * your class.
 */
@RunWith(StoreTestSuite.class)
public abstract class AbstractStoreITs {
    private final StoreProperties storeProperties;
    private final Schema schema;
    private final Collection<Class<? extends AbstractStoreIT>> extraTests;
    private final Map<Class<? extends AbstractStoreIT>, String> skipTests = new HashMap<>();
    private Class<? extends AbstractStoreIT> singleTestClass;
    private String singleTestMethod;

    public AbstractStoreITs(final StoreProperties storeProperties, final Schema schema, final Collection<Class<? extends AbstractStoreIT>> extraTests) {
        this.schema = schema;
        this.storeProperties = storeProperties;
        this.extraTests = extraTests;
    }

    public AbstractStoreITs(final StoreProperties storeProperties, final Collection<Class<? extends AbstractStoreIT>> extraTests) {
        this(storeProperties, new Schema(), extraTests);
    }

    public AbstractStoreITs(final StoreProperties storeProperties, final Schema schema) {
        this(storeProperties, schema, new ArrayList<Class<? extends AbstractStoreIT>>());
    }

    public AbstractStoreITs(final StoreProperties storeProperties) {
        this(storeProperties, new Schema());
    }

    public void singleTest(final Class<? extends AbstractStoreIT> testClass) {
        singleTest(testClass, null);
    }

    public void singleTest(final Class<? extends AbstractStoreIT> testClass, final String testMethod) {
        this.singleTestClass = testClass;
        this.singleTestMethod = testMethod;
    }

    public Schema getStoreSchema() {
        return schema;
    }

    public StoreProperties getStoreProperties() {
        return storeProperties;
    }

    public void addExtraTest(final Class<? extends AbstractStoreIT> extraTest) {
        extraTests.add(extraTest);
    }

    public Collection<? extends Class> getExtraTests() {
        return extraTests;
    }

    public Map<? extends Class<? extends AbstractStoreIT>, String> getSkipTests() {
        return skipTests;
    }

    protected void skipTest(final Class<? extends AbstractStoreIT> testClass, final String justification) {
        skipTests.put(testClass, justification);
    }

    public static class StoreTestSuite extends Suite {
        private Consumer<String> cleanUpFunction;

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
            AbstractStoreIT.setSingleTestMethod(runner.singleTestMethod);
        }

        private static Class[] getTestClasses(final Class<?> clazz) throws IllegalAccessException, InstantiationException {
            final AbstractStoreITs runner = clazz.asSubclass(AbstractStoreITs.class).newInstance();
            if (null == runner.singleTestClass) {
                final Set<Class<? extends AbstractStoreIT>> classes = new Reflections(AbstractStoreIT.class.getPackage().getName()).getSubTypesOf(AbstractStoreIT.class);
                keepPublicConcreteClasses(classes);
                classes.addAll((Collection<? extends Class<? extends AbstractStoreIT>>) runner.getExtraTests());
                return classes.toArray(new Class[classes.size()]);
            }

            return new Class[]{runner.singleTestClass};
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
