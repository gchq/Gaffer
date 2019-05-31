/*
 * Copyright 2017-2019 Crown Copyright
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

import com.google.common.collect.Sets;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewParameterDetail;
import uk.gov.gchq.gaffer.operation.OperationTest;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class AddNamedViewTest extends OperationTest<AddNamedView> {
    private static final String TEST_NAMED_VIEW_NAME = "testNamedViewName";
    private static final String TEST_DESCRIPTION = "testDescription";
    private static final View VIEW = new View.Builder()
            .edge(TestGroups.EDGE)
            .build();
    Map<String, ViewParameterDetail> parameters = new HashMap<>();

    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        parameters.put("testParameter", mock(ViewParameterDetail.class));

        AddNamedView addNamedView = new AddNamedView.Builder()
                .name(TEST_NAMED_VIEW_NAME)
                .view(VIEW)
                .description(TEST_DESCRIPTION)
                .parameters(parameters)
                .overwrite(true)
                .build();

        assertEquals(TEST_NAMED_VIEW_NAME, addNamedView.getName());
        JsonAssert.assertEquals(VIEW.toCompactJson(), addNamedView.getView().toCompactJson());
        assertTrue(addNamedView.isOverwriteFlag());
        assertEquals(parameters, addNamedView.getParameters());
        assertEquals(TEST_DESCRIPTION, addNamedView.getDescription());
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        parameters.put("testParameter", mock(ViewParameterDetail.class));

        AddNamedView addNamedView = new AddNamedView.Builder()
                .name(TEST_NAMED_VIEW_NAME)
                .view(VIEW)
                .description(TEST_DESCRIPTION)
                .parameters(parameters)
                .overwrite(false)
                .build();

        // When
        AddNamedView clone = addNamedView.shallowClone();

        // Then
        assertNotSame(addNamedView, clone);
        assertEquals(addNamedView.getName(), clone.getName());
        JsonAssert.assertEquals(addNamedView.getView().toJson(false), clone.getView().toJson(false));
        assertFalse(clone.isOverwriteFlag());
        assertEquals(addNamedView.getParameters(), clone.getParameters());
        assertEquals(addNamedView.getDescription(), clone.getDescription());
    }

    @Override
    protected AddNamedView getTestObject() {
        return new AddNamedView();
    }

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet("name", "view");
    }
}
