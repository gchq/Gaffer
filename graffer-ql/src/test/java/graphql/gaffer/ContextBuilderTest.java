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

package graphql.gaffer;

import gaffer.commonutil.StreamUtil;
import gaffer.example.films.analytic.LoadAndQuery;
import gaffer.example.films.data.Certificate;
import gaffer.graph.Graph;
import gaffer.graphql.GrafferQLContext;
import gaffer.graphql.GrafferQLException;
import gaffer.user.User;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Created by joe on 9/8/16.
 */
public class ContextBuilderTest {

    private User user;
    private Graph graph;

    @Before
    public void before() {
        // Setup User
        user = new User.Builder()
                .userId("user02")
                .dataAuth(Certificate.U.name())
                .dataAuth(Certificate.PG.name())
                .dataAuth(Certificate._12A.name())
                .dataAuth(Certificate._15.name())
                .dataAuth(Certificate._18.name())
                .build();

        // Setup graph
        graph = new Graph.Builder()
                .storeProperties(StreamUtil.openStream(LoadAndQuery.class, "/example/films/mockaccumulostore.properties"))
                .addSchemas(StreamUtil.openStreams(LoadAndQuery.class, "/example/films/schema"))
                .build();
    }

    @Test
    public void testValid() {
        try {
            final GrafferQLContext context = new GrafferQLContext.Builder()
                    .graph(graph)
                    .user(user)
                    .build();

            assertEquals(context.getGraph(), graph);
            assertEquals(context.getUser(), user);
        } catch (GrafferQLException e) {
            fail(e.getLocalizedMessage());
        }
    }

    @Test
    public void testNoGraph() {
        try {
            final GrafferQLContext context = new GrafferQLContext.Builder()
                    .user(user)
                    .build();

            fail("Should have thrown exception with missing graph");
        } catch (GrafferQLException e) {
            // goodie gumpdrops
        }
    }

    @Test
    public void testNoUser() {
        try {
            final GrafferQLContext context = new GrafferQLContext.Builder()
                    .graph(graph)
                    .build();

            fail("Should have thrown exception with missing user");
        } catch (GrafferQLException e) {
            // goodie gumpdrops
        }
    }
}
