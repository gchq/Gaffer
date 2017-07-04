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

package uk.gov.gchq.gaffer.flink.operation.handler;

import com.google.common.collect.Sets;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.TestTypes;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.OneToOneElementGenerator;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class FlinkTest {

    protected Graph createGraph() {
        return new Graph.Builder()
                .addSchemas(new Schema.Builder()
                        .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                                .clazz(String.class)
                                .build())
                        .type(TestTypes.PROP_COUNT, new TypeDefinition.Builder()
                                .clazz(Long.class)
                                .aggregateFunction(new Sum())
                                .build())
                        .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                                .vertex(TestTypes.ID_STRING)
                                .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                                .build())
                        .build())
                .storeProperties("store.properties")
                .build();
    }


    public static final java.util.HashSet<Entity> EXPECTED_ELEMENTS = Sets.newHashSet(
            new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("1")
                    .property(TestPropertyNames.COUNT, 1L)
                    .build(),
            new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("2")
                    .property(TestPropertyNames.COUNT, 1L)
                    .build(),
            new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("3")
                    .property(TestPropertyNames.COUNT, 1L)
                    .build()
    );

    protected void verifyElements(final Graph graph) throws OperationException {
        final Set<Element> allElements = Sets.newHashSet(graph.execute(new GetAllElements(), new User()));
        assertEquals(EXPECTED_ELEMENTS, allElements);
    }

    public static final class BasicGenerator implements OneToOneElementGenerator<String> {
        @Override
        public Element _apply(final String domainObject) {
            return new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex(domainObject)
                    .property(TestPropertyNames.COUNT, 1L)
                    .build();
        }
    }
}
