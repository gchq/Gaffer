/*
 * Copyright 2017-2018 Crown Copyright
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

import uk.gov.gchq.gaffer.operation.OperationTest;

import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

public class DeleteNamedViewTest extends OperationTest<DeleteNamedView> {
    private final String namedViewName = "testNamedViewName";

    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given / When
        DeleteNamedView operation = new DeleteNamedView.Builder().name(namedViewName).build();

        // Then
        assertEquals(namedViewName, operation.getName());
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        DeleteNamedView deleteNamedView = new DeleteNamedView.Builder().name(namedViewName).build();

        // When
        DeleteNamedView clone = deleteNamedView.shallowClone();

        // Then
        assertNotSame(deleteNamedView, clone);
        assertEquals(namedViewName, clone.getName());
    }

    @Override
    protected DeleteNamedView getTestObject() {
        return new DeleteNamedView();
    }

    @Override
    protected Set<String> getRequiredFields() {
        return Collections.singleton("name");
    }
}
