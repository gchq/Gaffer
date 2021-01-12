/*
 * Copyright 2020 Crown Copyright
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

package uk.gov.gchq.gaffer.integration.template.loader;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.integration.GafferTest;
import uk.gov.gchq.gaffer.integration.extensions.LoaderTestCase;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.user.User;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AddElementsLoaderITTemplate extends AbstractLoaderIT {


    @Override
    public void addElements(final Graph graph, final Iterable<? extends Element> input) throws OperationException {
        graph.execute(new AddElements.Builder()
            .input(input)
            .build(), new User());
    }

    //////////////////////////////////////////////////////////////////
    //                  Add Elements error handling                 //
    //////////////////////////////////////////////////////////////////
    @GafferTest
    public void shouldThrowExceptionWithUsefulMessageWhenInvalidElementsAdded(final LoaderTestCase testCase) throws OperationException {
        // Given
        Graph graph = testCase.getGraph();

        // When
        final AddElements addElements = new AddElements.Builder()
            .input(new Edge("UnknownGroup", "source", "dest", true))
            .build();

        // Then
        Exception e = assertThrows(Exception.class, () -> graph.execute(addElements, new User()));

        String msg = e.getMessage();
        if (!msg.contains("Element of type Entity") && null != e.getCause()) {
            msg = e.getCause().getMessage();
        }
        assertTrue(msg.contains("UnknownGroup"), "Message was: " + msg);
    }

    @GafferTest
    public void shouldNotThrowExceptionWhenInvalidElementsAddedWithSkipInvalidSetToTrue(final LoaderTestCase testCase) throws OperationException {
        // Given
        Graph graph = testCase.getGraph();

        final AddElements addElements = new AddElements.Builder()
            .input(new Edge("Unknown group", "source", "dest", true))
            .skipInvalidElements(true)
            .build();

        // When
        graph.execute(addElements, new User());

        // Then - no exceptions
    }

    @GafferTest
    public void shouldNotThrowExceptionWhenInvalidElementsAddedWithValidateSetToFalse(final LoaderTestCase testCase) throws OperationException {
        // Given
        Graph graph = testCase.getGraph();
        final AddElements addElements = new AddElements.Builder()
            .input(new Edge("Unknown group", "source", "dest", true))
            .validate(false)
            .build();

        // When
        graph.execute(addElements, new User());

        // Then - no exceptions
    }
}
