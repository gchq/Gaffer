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
package uk.gov.gchq.gaffer.store.operation.handler.function;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.function.Transform;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.koryphe.impl.function.Identity;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TransformHandlerTest {
    @Test
    public void shouldTransformElementsUsingIdentityFunction() throws OperationException {
        // Given
        final List<Element> input = new ArrayList<>();
        final List<Element> expected = new ArrayList<>();

        final Store store = mock(Store.class);
        final Context context = new Context();
        final TransformHandler handler = new TransformHandler();

        final Entity entity = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .property(TestPropertyNames.PROP_1, TestPropertyNames.INT)
                .property(TestPropertyNames.PROP_2, TestPropertyNames.STRING)
                .build();

        final Entity entity1 = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .property(TestPropertyNames.PROP_1, TestPropertyNames.INT)
                .build();

        final ElementTransformer transformer = new ElementTransformer.Builder()
                .select(TestPropertyNames.PROP_1)
                .execute(new Identity())
                .project(TestPropertyNames.PROP_3)
                .build();

        final Entity expectedEntity = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .property(TestPropertyNames.PROP_3, TestPropertyNames.INT)
                .property(TestPropertyNames.PROP_2, TestPropertyNames.STRING)
                .build();

        final Entity expectedEntity1 = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .property(TestPropertyNames.PROP_3, TestPropertyNames.INT)
                .build();

        final Transform transform = new Transform.Builder()
                .input(input)
                .elementTransformer(transformer)
                .build();

        input.add(entity);
        input.add(entity1);

        expected.add(expectedEntity);
        expected.add(expectedEntity1);

        // When
        final Iterable<? extends Element> results = handler.doOperation(transform, context, store);
        final List<Element> resultsList = Streams.toStream(results).collect(Collectors.toList());

        assertTrue(check(expected, resultsList));
    }

    private boolean check(final List<Element> expected, final List<Element> resultsList) {
        boolean isSame = false;
        for (int i = 0; i < expected.size(); i++) {
            isSame = expected.get(i).getProperty(TestPropertyNames.PROP_3).equals(resultsList.get(i).getProperty(TestPropertyNames.PROP_3));
        }
        return isSame;
    }
}
