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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import com.google.common.collect.Lists;
import gaffer.commonutil.TestGroups;
import gaffer.commonutil.TestPropertyNames;
import gaffer.data.element.Element;
import gaffer.data.element.function.ElementTransformer;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewElementDefinition;
import gaffer.function.ExampleStringTransformFunction;
import gaffer.function.simple.aggregate.BooleanAnd;
import gaffer.function.simple.transform.Remove;
import gaffer.integration.AbstractStoreIT;
import gaffer.operation.OperationException;
import gaffer.operation.data.ElementSeed;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.get.GetElementsSeed;
import gaffer.operation.simple.UpdateElements;
import gaffer.store.schema.Schema;
import gaffer.store.schema.SchemaEntityDefinition;
import gaffer.store.schema.TypeDefinition;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Before;
import org.junit.Test;
import java.io.UnsupportedEncodingException;
import java.util.List;

public class UpdateElementsIT extends AbstractStoreIT {
    private final String UPDATED_ELEMENT_VERTEX = "A1";

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        addDefaultElements();
    }

    @Override
    protected Schema createSchema() {
        return new Schema.Builder()
                .type("boolean", new TypeDefinition.Builder()
                        .aggregateFunction(new BooleanAnd())
                        .clazz(Boolean.class)
                        .build())
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .property("UPDATED_ELEMENT", "boolean")
                        .build())
                .merge(super.createSchema())
                .build();
    }

    @Test
    public void shouldTransformElements() throws OperationException, UnsupportedEncodingException {
        // Given
        final UpdateElements updateElements = new UpdateElements.Builder()
                .addSeed(new EntitySeed(UPDATED_ELEMENT_VERTEX))
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                                .transformer(new ElementTransformer.Builder()
                                        .select(TestPropertyNames.STRING)
                                        .project(TestPropertyNames.STRING)
                                        .execute(new ExampleStringTransformFunction())
                                        .build())
                                .build())
                        .build())
                .build();

        // When
        graph.execute(updateElements);

        // Then - check the elements have been updated
        final GetElementsSeed<ElementSeed, Element> getElementsBySeed = new GetElementsSeed.Builder<>()
                .addSeed(new EntitySeed(UPDATED_ELEMENT_VERTEX))
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .build();

        final List<Element> updatedElements = Lists.newArrayList(graph.execute(getElementsBySeed));
        assertNotNull(updatedElements);
        assertEquals(1, updatedElements.size());
        assertThat(updatedElements, IsCollectionContaining.hasItems(
                (Element) getEntity(UPDATED_ELEMENT_VERTEX)
        ));
        assertEquals("0 transformed", updatedElements.get(0).getProperty(TestPropertyNames.STRING));
    }

    @Test
    public void shouldRemoveElementProperty() throws OperationException, UnsupportedEncodingException {
        // Given
        final UpdateElements updateElements = new UpdateElements.Builder()
                .addSeed(new EntitySeed(UPDATED_ELEMENT_VERTEX))
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                                .transformer(new ElementTransformer.Builder()
                                        .select(TestPropertyNames.STRING)
                                        .project(TestPropertyNames.STRING)
                                        .execute(new Remove())
                                        .build())
                                .build())
                        .build())
                .build();

        // When
        graph.execute(updateElements);

        // Then - check the elements have been updated
        final GetElementsSeed<ElementSeed, Element> getElementsBySeed = new GetElementsSeed.Builder<>()
                .addSeed(new EntitySeed(UPDATED_ELEMENT_VERTEX))
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .build();

        final List<Element> updatedElements = Lists.newArrayList(graph.execute(getElementsBySeed));
        assertNotNull(updatedElements);
        assertEquals(1, updatedElements.size());
        assertThat(updatedElements, IsCollectionContaining.hasItems(
                (Element) getEntity(UPDATED_ELEMENT_VERTEX)
        ));
        assertFalse(updatedElements.get(0).getProperties().containsKey(TestPropertyNames.STRING));
    }
}
