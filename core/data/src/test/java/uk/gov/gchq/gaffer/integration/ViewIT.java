/*
 * Copyright 2016-2018 Crown Copyright
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
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;

import java.io.IOException;

import static org.junit.Assert.assertEquals;


public class ViewIT {
//    @Test
//    public void shouldDeserialiseJsonView() throws IOException {
//        // Given
//
//        // When
//        View view = loadView();
//
//        // Then
//        final ViewElementDefinition edge = view.getEdge(TestGroups.EDGE);
//        final ElementTransformer transformer = edge.getTransformer();
//        assertNotNull(transformer);
//
//        final List<Function> contexts = transformer.getFunctions();
//        assertEquals(1, contexts.size());
//
//        final List<String> selection = contexts.get(0).getSelection();
//        assertEquals(2, selection.size());
//        assertEquals(TestPropertyNames.PROP_1, selection.get(0));
//        assertEquals(IdentifierType.SOURCE.name(), selection.get(1));
//
//        final List<String> projection = contexts.get(0).getProjection();
//        assertEquals(1, projection.size());
//        assertEquals(TestPropertyNames.TRANSIENT_1, projection.get(0));
//
//        assertTrue(contexts.get(0).getPredicate() instanceof ExampleTransformFunction);
//
//        final ElementFilter postFilter = edge.getPostTransformFilter();
//        assertNotNull(postFilter);
//
//        final List<ConsumerFunctionContext<String, Predicate>> filterContexts = postFilter.getFunctions();
//        assertEquals(1, contexts.size());
//
//        final List<String> postFilterSelection = filterContexts.get(0).getSelection();
//        assertEquals(1, postFilterSelection.size());
//        assertEquals(TestPropertyNames.TRANSIENT_1, postFilterSelection.get(0));
//
//        assertTrue(filterContexts.get(0).getPredicate() instanceof ExampleFilterFunction);
//
//
//    }

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
