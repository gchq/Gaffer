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

package gaffer.rest.service;

import gaffer.commonutil.TestGroups;
import gaffer.data.elementdefinition.schema.DataEdgeDefinition;
import gaffer.data.elementdefinition.schema.DataEntityDefinition;
import gaffer.data.elementdefinition.schema.DataSchema;
import gaffer.graph.Graph;
import gaffer.jsonserialisation.JSONSerialiser;
import gaffer.operation.Operation;
import gaffer.rest.GraphFactory;
import gaffer.store.Store;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;


public class ExamplesServiceTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();
    private SimpleExamplesService service;

    @Before
    public void setup() {
        final DataSchema dataSchema = new DataSchema.Builder()
                .entity(TestGroups.ENTITY, new DataEntityDefinition.Builder()
                        .property("entityProperties", String.class)
                        .vertex(String.class)
                        .build())
                .edge(TestGroups.EDGE, new DataEdgeDefinition.Builder()
                        .property("edgeProperties", String.class)
                        .source(String.class)
                        .destination(String.class)
                        .directed(Boolean.class)
                        .build())
                .build();

        final GraphFactory graphFactory = mock(GraphFactory.class);
        final Store store = mock(Store.class);
        given(store.getDataSchema()).willReturn(dataSchema);
        final Graph graph = new Graph(store);
        given(graphFactory.getGraph()).willReturn(graph);

        service = new SimpleExamplesService(graphFactory);
    }

    @Test
    public void shouldSerialiseAndDeserialiseAddElements() throws IOException {
        shouldSerialiseAndDeserialiseOperation(service.addElements());
    }

    @Test
    public void shouldSerialiseAndDeserialiseGetElementsBySeed() throws IOException {
        shouldSerialiseAndDeserialiseOperation(service.getElementsBySeed());
    }

    @Test
    public void shouldSerialiseAndDeserialiseGetRelatedElements() throws IOException {
        shouldSerialiseAndDeserialiseOperation(service.getRelatedElements());
    }

    @Test
    public void shouldSerialiseAndDeserialiseGetEntitiesBySeed() throws IOException {
        shouldSerialiseAndDeserialiseOperation(service.getEntitiesBySeed());
    }

    @Test
    public void shouldSerialiseAndDeserialiseGetRelatedEntities() throws IOException {
        shouldSerialiseAndDeserialiseOperation(service.getRelatedEntities());
    }

    @Test
    public void shouldSerialiseAndDeserialiseGetEdgesBySeed() throws IOException {
        shouldSerialiseAndDeserialiseOperation(service.getEdgesBySeed());
    }

    @Test
    public void shouldSerialiseAndDeserialiseGetRelatedEdges() throws IOException {
        shouldSerialiseAndDeserialiseOperation(service.getRelatedEdges());
    }

    private void shouldSerialiseAndDeserialiseOperation(Operation operation) throws IOException {
        //Given

        // When
        byte[] bytes = serialiser.serialise(operation);
        final Operation deserialisedOp = serialiser.deserialise(bytes, operation.getClass());

        // Then
        assertNotNull(deserialisedOp);
    }
}
