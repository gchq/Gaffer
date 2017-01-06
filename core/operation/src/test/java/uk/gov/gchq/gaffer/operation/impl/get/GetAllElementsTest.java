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

package uk.gov.gchq.gaffer.operation.impl.get;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.GetOperation;
import uk.gov.gchq.gaffer.operation.OperationTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;


public class GetAllElementsTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        // Given
        final GetAllElements op = new GetAllElements();

        // When
        byte[] json = serialiser.serialise(op, true);
        final GetAllElements deserialisedOp = serialiser.deserialise(json, GetAllElements.class);

        // Then
        assertNotNull(deserialisedOp);
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        GetAllElements<Element> getAllElements = new GetAllElements.Builder<>()
                .includeEdges(GetOperation.IncludeEdgeType.ALL)
                .includeEntities(false)
                .option("testOption", "true")
                .populateProperties(false)
                .view(new View.Builder()
                        .edge(TestGroups.EDGE)
                        .build())
                .build();

        assertFalse(getAllElements.isIncludeEntities());
        assertFalse(getAllElements.isPopulateProperties());
        assertEquals(GetOperation.IncludeEdgeType.ALL, getAllElements.getIncludeEdges());
        assertEquals("true", getAllElements.getOption("testOption"));
        assertNotNull(getAllElements.getView().getEdge(TestGroups.EDGE));
    }
}
