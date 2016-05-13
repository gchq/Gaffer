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

package gaffer.operation.impl.get;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import gaffer.commonutil.TestGroups;
import gaffer.data.elementdefinition.view.View;
import gaffer.exception.SerialisationException;
import gaffer.jsonserialisation.JSONSerialiser;
import gaffer.operation.OperationTest;
import org.junit.Test;


public class GetAllEdgesTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        // Given
        final GetAllEdges op = new GetAllEdges();

        // When
        byte[] json = serialiser.serialise(op, true);
        final GetAllEdges deserialisedOp = serialiser.deserialise(json, GetAllEdges.class);

        // Then
        assertNotNull(deserialisedOp);
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        GetAllEdges getAllEdges = new GetAllEdges.Builder()
                .option("testOption", "true")
                .populateProperties(false)
                .summarise(true)
                .view(new View.Builder()
                        .edge(TestGroups.EDGE)
                        .build())
                .build();

        assertTrue(getAllEdges.isSummarise());
        assertFalse(getAllEdges.isPopulateProperties());
        assertEquals("true", getAllEdges.getOption("testOption"));
        assertTrue(getAllEdges.isSummarise());
        assertNotNull(getAllEdges.getView().getEdge(TestGroups.EDGE));
    }
}
