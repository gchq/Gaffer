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

package uk.gov.gchq.gaffer.named.view;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.elementdefinition.view.NamedView;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewParameterDetail;
import uk.gov.gchq.gaffer.operation.OperationTest;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class AddNamedViewTest extends OperationTest<AddNamedView> {
    private static final String TEST_NAMED_VIEW_NAME = "testNamedViewName";
    private static final String testDescription = "testDescription";
    private static final NamedView NAMED_VIEW = new NamedView.Builder()
            .name(TEST_NAMED_VIEW_NAME)
            .edge(TestGroups.EDGE)
            .build();
    Map<String, ViewParameterDetail> parameters = new HashMap<>();

    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        parameters.put("testParameter", mock(ViewParameterDetail.class));

        AddNamedView addNamedView = new AddNamedView.Builder()
                .namedView(NAMED_VIEW)
                .description(testDescription)
                .parameters(parameters)
                .overwrite(true)
                .build();

        assertEquals(NAMED_VIEW, addNamedView.getNamedView());
        assertTrue(addNamedView.isOverwriteFlag());
        assertEquals(parameters, addNamedView.getParameters());
        assertEquals(testDescription, addNamedView.getDescription());
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        parameters.put("testParameter", mock(ViewParameterDetail.class));

        System.out.println(new String(NAMED_VIEW.toJson(true)));

        AddNamedView addNamedView = new AddNamedView.Builder()
                .namedView(NAMED_VIEW)
                .description(testDescription)
                .parameters(parameters)
                .overwrite(false)
                .build();

        // When
        AddNamedView clone = addNamedView.shallowClone();

        // Then
        assertNotSame(addNamedView, clone);
        JsonAssert.assertEquals(addNamedView.getNamedView().toJson(false), clone.getNamedView().toJson(false));
        assertFalse(clone.isOverwriteFlag());
        assertEquals(parameters, addNamedView.getParameters());
        assertEquals(testDescription, addNamedView.getDescription());
    }

    @Override
    protected AddNamedView getTestObject() {
        return new AddNamedView.Builder()
                .namedView(NAMED_VIEW)
                .build();
    }
}
