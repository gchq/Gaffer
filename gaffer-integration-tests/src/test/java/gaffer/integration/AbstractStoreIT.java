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

import gaffer.commonutil.TestGroups;
import gaffer.commonutil.TestPropertyNames;
import gaffer.function.simple.aggregate.StringConcat;
import gaffer.function.simple.aggregate.Sum;
import gaffer.graph.Graph;
import gaffer.serialisation.simple.IntegerSerialiser;
import gaffer.serialisation.simple.LongSerialiser;
import gaffer.serialisation.simple.StringSerialiser;
import gaffer.store.StoreProperties;
import gaffer.store.StoreTrait;
import gaffer.store.schema.Schema;
import gaffer.store.schema.SchemaEdgeDefinition;
import gaffer.store.schema.SchemaEntityDefinition;
import gaffer.store.schema.TypeDefinition;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

public abstract class AbstractStoreIT {
    protected static Graph graph;
    private static Schema storeSchema = new Schema();
    private static StoreProperties storeProperties;

    @Rule
    public TestName name = new TestName();


    public static void setStoreProperties(final StoreProperties storeProperties) {
        AbstractStoreIT.storeProperties = storeProperties;
    }

    public static StoreProperties getStoreProperties() {
        return storeProperties;
    }

    public static Schema getStoreSchema() {
        return storeSchema;
    }

    public static void setStoreSchema(final Schema storeSchema) {
        AbstractStoreIT.storeSchema = storeSchema;
    }

    /**
     * Setup the Parameterised Graph for each type of Store.
     * Excludes tests where the graph's Store doesn't implement the required StoreTraits.
     *
     * @throws Exception should never be thrown
     */
    @Before
    public void setup() throws Exception {
        assumeTrue("Skipping test as no store properties have been defined.", null != storeProperties);

        graph = new Graph.Builder()
                .storeProperties(storeProperties)
                .addSchema(new Schema.Builder()
                        .type("id.string", new TypeDefinition.Builder()
                                .clazz(String.class)
                                .build())
                        .type("directed.either", new TypeDefinition.Builder()
                                .clazz(Boolean.class)
                                .build())
                        .type("prop.string", new TypeDefinition.Builder()
                                .clazz(String.class)
                                .aggregateFunction(new StringConcat())
                                .serialiser(new StringSerialiser())
                                .build())
                        .type("prop.integer", new TypeDefinition.Builder()
                                .clazz(Integer.class)
                                .aggregateFunction(new Sum())
                                .serialiser(new IntegerSerialiser())
                                .build())
                        .type("prop.count", new TypeDefinition.Builder()
                                .clazz(Long.class)
                                .aggregateFunction(new Sum())
                                .serialiser(new LongSerialiser())
                                .build())
                        .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                                .vertex("id.string")
                                .property(TestPropertyNames.STRING, "prop.string")
                                .build())
                        .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                                .source("id.string")
                                .destination("id.string")
                                .directed("directed.either")
                                .property(TestPropertyNames.INT, "prop.integer")
                                .property(TestPropertyNames.COUNT, "prop.count")
                                .build())
                        .vertexSerialiser(new StringSerialiser())
                        .build())
                .addSchema(storeSchema)
                .build();

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


}
