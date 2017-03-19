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

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.function.ExampleFilterFunction;
import uk.gov.gchq.gaffer.function.ExampleTransformFunction;
import uk.gov.gchq.gaffer.function.FilterFunction;
import uk.gov.gchq.gaffer.function.TransformFunction;
import uk.gov.gchq.gaffer.function.context.ConsumerFunctionContext;
import uk.gov.gchq.gaffer.function.context.ConsumerProducerFunctionContext;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class ViewIT {
    @Test
    public void shouldDeserialiseJsonView() throws IOException {
        // Given

        // When
        View view = loadView();

        // Then
        final ViewElementDefinition edge = view.getEdge(TestGroups.EDGE);
        final ElementTransformer transformer = edge.getTransformer();
        assertNotNull(transformer);

        final List<ConsumerProducerFunctionContext<String, TransformFunction>> contexts = transformer.getFunctions();
        assertEquals(1, contexts.size());

        final List<String> selection = contexts.get(0).getSelection();
        assertEquals(2, selection.size());
        assertEquals(TestPropertyNames.PROP_1, selection.get(0));
        assertEquals(IdentifierType.SOURCE.name(), selection.get(1));

        final List<String> projection = contexts.get(0).getProjection();
        assertEquals(1, projection.size());
        assertEquals(TestPropertyNames.TRANSIENT_1, projection.get(0));

        assertTrue(contexts.get(0).getFunction() instanceof ExampleTransformFunction);

        final ElementFilter postFilter = edge.getPostTransformFilter();
        assertNotNull(postFilter);

        final List<ConsumerFunctionContext<String, FilterFunction>> filterContexts = postFilter.getFunctions();
        assertEquals(1, contexts.size());

        final List<String> postFilterSelection = filterContexts.get(0).getSelection();
        assertEquals(1, postFilterSelection.size());
        assertEquals(TestPropertyNames.TRANSIENT_1, postFilterSelection.get(0));

        assertTrue(filterContexts.get(0).getFunction() instanceof ExampleFilterFunction);


    }

    @Test
    public void shouldDeserialiseAndReserialiseIntoTheSameJson() throws IOException {
        //Given
        final View view1 = loadView();
        final byte[] json1 = view1.toCompactJson();
        final View view2 = new View.Builder().json(json1).build();

        // When
        final byte[] json2 = view2.toCompactJson();

        // Then
        assertEquals(new String(json1), new String(json2));
    }

    @Test
    public void shouldDeserialiseAndReserialiseIntoTheSamePrettyJson() throws IOException {
        //Given
        final View view1 = loadView();
        final byte[] json1 = view1.toJson(true);
        final View view2 = new View.Builder().json(json1).build();

        // When
        final byte[] json2 = view2.toJson(true);

        // Then
        assertEquals(new String(json1), new String(json2));
    }

    private View loadView() throws IOException {
        return new View.Builder().json(StreamUtil.view(getClass())).build();
    }
}
