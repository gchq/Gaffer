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

package uk.gov.gchq.gaffer.data.elementdefinition.view;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class ViewElementDefinitionTest {
    @Test
    public void shouldBuildElementDefinition() {
        // Given
        final ElementTransformer transformer = mock(ElementTransformer.class);
        final ElementFilter filter = mock(ElementFilter.class);
        final ElementFilter postFilter = mock(ElementFilter.class);


        // When
        final ViewElementDefinition elementDef = new ViewElementDefinition.Builder()
                .transientProperty(TestPropertyNames.PROP_1, String.class)
                .transientProperty(TestPropertyNames.PROP_2, String.class)
                .transformer(transformer)
                .preAggregationFilter(filter)
                .postTransformFilter(postFilter)
                .build();

        // Then
        assertEquals(2, elementDef.getTransientProperties().size());
        assertTrue(elementDef.containsTransientProperty(TestPropertyNames.PROP_1));
        assertTrue(elementDef.containsTransientProperty(TestPropertyNames.PROP_2));

        assertSame(filter, elementDef.getPreAggregationFilter());
        assertSame(postFilter, elementDef.getPostTransformFilter());
        assertSame(transformer, elementDef.getTransformer());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToBuildElementDefinitionWhenPreAggregationFilterSpecifiedTwice() {
        // Given
        final ElementTransformer transformer = mock(ElementTransformer.class);
        final ElementFilter filter = mock(ElementFilter.class);

        // When
        final ViewElementDefinition elementDef = new ViewElementDefinition.Builder()
                .transientProperty(TestPropertyNames.PROP_1, String.class)
                .transientProperty(TestPropertyNames.PROP_2, String.class)
                .transformer(transformer)
                .preAggregationFilter(filter)
                .preAggregationFilter(filter)
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToBuildElementDefinitionWhenPostAggregationFilterSpecifiedTwice() {
        // Given
        final ElementTransformer transformer = mock(ElementTransformer.class);
        final ElementFilter filter = mock(ElementFilter.class);

        // When
        final ViewElementDefinition elementDef = new ViewElementDefinition.Builder()
                .transientProperty(TestPropertyNames.PROP_1, String.class)
                .transientProperty(TestPropertyNames.PROP_2, String.class)
                .transformer(transformer)
                .postAggregationFilter(filter)
                .postAggregationFilter(filter)
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToBuildElementDefinitionWhenPostTransformFilterSpecifiedTwice() {
        // Given
        final ElementTransformer transformer = mock(ElementTransformer.class);
        final ElementFilter postFilter = mock(ElementFilter.class);

        // When
        final ViewElementDefinition elementDef = new ViewElementDefinition.Builder()
                .transientProperty(TestPropertyNames.PROP_1, String.class)
                .transientProperty(TestPropertyNames.PROP_2, String.class)
                .transformer(transformer)
                .postTransformFilter(postFilter)
                .postTransformFilter(postFilter)
                .build();
    }
}