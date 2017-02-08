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

package uk.gov.gchq.gaffer.example.films.analytic;

import com.google.common.collect.Lists;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.example.films.data.Certificate;
import uk.gov.gchq.gaffer.example.films.data.schema.Group;
import uk.gov.gchq.gaffer.example.films.data.schema.Property;
import uk.gov.gchq.gaffer.example.films.data.schema.TransientProperty;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.user.User;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LoadAndQueryTest {
    @Test
    public void shouldReturnExpectedEntities() throws OperationException {
        // Given
        final LoadAndQuery query = new LoadAndQuery();

        // When
        final CloseableIterable<Entity> results = query.run();

        // Then
        verifyResults(results);
    }


    @Test
    public void shouldReturnExpectedEntitiesViaJson() throws OperationException, SerialisationException {
        // Given
        final User user = new User.Builder()
                .userId("user02")
                .dataAuth(Certificate.U.name())
                .dataAuth(Certificate.PG.name())
                .dataAuth(Certificate._12A.name())
                .dataAuth(Certificate._15.name())
                .dataAuth(Certificate._18.name())
                .build();
        final JSONSerialiser serialiser = new JSONSerialiser();
        final OperationChain<Void> populateChain = serialiser.deserialise(StreamUtil.openStream(LoadAndQuery.class, "/example/films/json/load.json"), OperationChain.class);
        final OperationChain<CloseableIterable<Entity>> queryChain = serialiser.deserialise(StreamUtil.openStream(LoadAndQuery.class, "/example/films/json/query.json"), OperationChain.class);

        // Setup graph
        final Graph graph = new Graph.Builder()
                .storeProperties(StreamUtil.openStream(LoadAndQuery.class, "/example/films/mockaccumulostore.properties"))
                .addSchemas(StreamUtil.openStreams(LoadAndQuery.class, "/example/films/schema"))
                .build();

        // When
        graph.execute(populateChain, user); // Execute the populate operation chain on the graph
        final CloseableIterable<Entity> results = graph.execute(queryChain, user); // Execute the query operation chain on the graph.

        // Then
        verifyResults(results);
    }

    private void verifyResults(final CloseableIterable<Entity> resultsItr) {
        final List<Entity> expectedResults = new ArrayList<>();
        final Entity entity = new Entity(Group.REVIEW, "filmA");
        entity.putProperty(Property.USER_ID, "user01,user03");
        entity.putProperty(Property.RATING, 100L);
        entity.putProperty(Property.COUNT, 2);
        entity.putProperty(TransientProperty.FIVE_STAR_RATING, 2.5F);
        expectedResults.add(entity);

        final List<Entity> results = Lists.newArrayList(resultsItr);
        assertEquals(expectedResults.size(), results.size());
        for (int i = 0; i < expectedResults.size(); i++) {
            assertTrue(expectedResults.get(i).equals(results.get(i)));
        }
    }

}