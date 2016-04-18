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
package gaffer.operation.integration.simple;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import com.google.common.collect.Lists;
import gaffer.function.ExampleIntegerTransformFunction;
import gaffer.function.ExampleStringTransformFunction;
import gaffer.commonutil.TestGroups;
import gaffer.commonutil.TestPropertyNames;
import gaffer.commonutil.TestTypes;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.data.element.function.ElementTransformer;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewElementDefinition;
import gaffer.integration.AbstractStoreIT;
import gaffer.operation.OperationException;
import gaffer.operation.data.EdgeSeed;
import gaffer.operation.data.ElementSeed;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.get.GetElementsSeed;
import gaffer.operation.simple.UpdateStore;
import gaffer.store.schema.Schema;
import gaffer.store.schema.SchemaEntityDefinition;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Before;
import org.junit.Test;
import java.io.UnsupportedEncodingException;
import java.util.List;

public class UpdateStoreIT extends AbstractStoreIT {
    public static final String NEW_ENTITY_GROUP = "newEntityGroup";
    public static final String NEW_PROPERTY = TestPropertyNames.PROP_1;

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        addDefaultElements();
    }

    @Test
    public void shouldUpdateSchema() throws OperationException, UnsupportedEncodingException {
        // Given
        final Schema newSchema = new Schema.Builder()
                .entity(NEW_ENTITY_GROUP)
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .property(NEW_PROPERTY, TestTypes.PROP_STRING)
                        .build())
                .merge(graph.getSchema())
                .build();

        final UpdateStore updateStore = new UpdateStore.Builder()
                .newSchema(newSchema)
                .build();

        // When
        graph.execute(updateStore);

        // Then - check schema has been updated.
        assertNotNull(graph.getSchema().getEntity(NEW_ENTITY_GROUP));
    }

    @Test
    public void shouldTransformAllElementsAndUpdateSchema() throws OperationException, UnsupportedEncodingException {
        // Given
        final Schema newSchema = new Schema.Builder()
                .entity(NEW_ENTITY_GROUP)
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .property(NEW_PROPERTY, TestTypes.PROP_STRING)
                        .build())
                .merge(graph.getSchema())
                .build();

        final UpdateStore updateStore = new UpdateStore.Builder()
                .newSchema(newSchema)
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                                .transientProperty(NEW_PROPERTY, String.class)
                                .transformer(new ElementTransformer.Builder()
                                        .select(TestPropertyNames.STRING)
                                        .project(NEW_PROPERTY)
                                        .execute(new ExampleStringTransformFunction())
                                        .build())
                                .build())
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .transformer(new ElementTransformer.Builder()
                                        .select(TestPropertyNames.INT)
                                        .project(TestPropertyNames.INT)
                                        .execute(new ExampleIntegerTransformFunction())
                                        .build())
                                .build())
                        .build())
                .build();

        // When
        graph.execute(updateStore);

        // Then - check the elements have been migrated
        final GetElementsSeed<ElementSeed, Element> getElementsBySeed = new GetElementsSeed.Builder<>()
                .addSeed(new EntitySeed("A1"))
                .addSeed(new EdgeSeed(SOURCE_1, DEST_1, false))
                .build();

        final List<Element> migratedElements = Lists.newArrayList(graph.execute(getElementsBySeed));
        assertNotNull(migratedElements);
        assertEquals(2, migratedElements.size());
        assertThat(migratedElements, IsCollectionContaining.hasItems(
                getEntity("A1"),
                getEdge(SOURCE_1, DEST_1, false)
        ));
        for (Element migratedElement : migratedElements) {
            if (migratedElement instanceof Entity) {
                assertEquals("0", migratedElement.getProperty(TestPropertyNames.STRING));
                assertEquals("0 transformed", migratedElement.getProperty(NEW_PROPERTY));
            } else {
                assertEquals(1001, (int) migratedElement.getProperty(TestPropertyNames.INT));
            }
        }

        // Then - check schema has been updated.
        assertNotNull(graph.getSchema().getEntity(NEW_ENTITY_GROUP));
    }
}
