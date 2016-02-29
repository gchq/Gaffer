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

import static org.junit.Assume.assumeTrue;

import gaffer.commonutil.StreamUtil;
import gaffer.graph.Graph;
import gaffer.store.StoreTrait;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public abstract class GafferIntegrationTests {

    protected static Graph graph;

    @Rule
    public TestName name = new TestName();
    @Parameterized.Parameter(0)
    public String storeName;
    @Parameterized.Parameter(1)
    public String propertiesPath;

    /**
     * Setup the Parameterised Graph for each type of Store.
     * Excludes tests where the graph's Store doesn't implement the required StoreTraits.
     *
     * @throws Exception should never be thrown
     */
    @Before
    public void setup() throws Exception {
        graph = new Graph(StreamUtil.dataSchema(getClass()), StreamUtil.storeSchema(getClass()), StreamUtil.openStream(GafferIntegrationTests.class, propertiesPath));

        final String originalMethodName = name.getMethodName().endsWith("]")
                ? name.getMethodName().substring(0, name.getMethodName().indexOf("["))
                : name.getMethodName();
        final Method testMethod = this.getClass().getMethod(originalMethodName);
        final Collection<StoreTrait> requiredTraits = new ArrayList<>();

        for (Annotation annotation : testMethod.getDeclaredAnnotations()) {
            if (annotation.annotationType().equals(gaffer.integration.TraitRequirement.class)) {
                final gaffer.integration.TraitRequirement traitRequirement = (gaffer.integration.TraitRequirement) annotation;
                requiredTraits.addAll(Arrays.asList(traitRequirement.value()));
            }
        }

        for (StoreTrait requiredTrait : requiredTraits) {
            assumeTrue("Skipping test as the store does not implement all required traits.", graph.hasTrait(requiredTrait));
        }
    }

    @After
    public void tearDown() {
        graph = null;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> runnables() throws IOException {

        // Different Stores/Graphs
        return Arrays.asList(
                new Object[][]{
                        {"AccumuloStore", "/accumulo.properties"},
                        {"ArrayListStore", "/arraylist.properties"}
                }
        );
    }
}
