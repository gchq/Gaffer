/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.service;

import org.glassfish.jersey.client.ChunkedInput;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.GroupCounts;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.CountGroups;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.rest.AbstractRestApiIT;
import uk.gov.gchq.gaffer.rest.RestApiTestUtil;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class OperationServiceIT extends AbstractRestApiIT {
    @Test
    public void shouldReturnAllElements() throws IOException {
        // Given
        RestApiTestUtil.addElements(DEFAULT_ELEMENTS);

        // When
        final Response response = RestApiTestUtil.executeOperation(new GetAllElements<>());

        // Then
        final List<Element> results = response.readEntity(new GenericType<List<Element>>() {
        });

        verifyElements(DEFAULT_ELEMENTS, results);
    }

    @Test
    public void shouldReturnGroupCounts() throws IOException {
        // Given
        RestApiTestUtil.addElements(DEFAULT_ELEMENTS);

        // When
        final Response response = RestApiTestUtil.executeOperationChainChunked(new OperationChain.Builder()
                .first(new GetAllElements<>())
                .then(new CountGroups())
                .build());

        // Then
        final GroupCounts groupCounts = response.readEntity(new GenericType<GroupCounts>() {
        });

        verifyGroupCounts(groupCounts);
    }

    @Test
    public void shouldReturnChunkedElements() throws IOException {
        // Given
        RestApiTestUtil.addElements(DEFAULT_ELEMENTS);

        // When
        final Response response = RestApiTestUtil.executeOperationChainChunked(new OperationChain<>(new GetAllElements<>()));

        // Then
        final List<Element> results = readChunkedElements(response);
        verifyElements(DEFAULT_ELEMENTS, results);
    }

    @Test
    public void shouldReturnChunkedGroupCounts() throws IOException {
        // Given
        RestApiTestUtil.addElements(DEFAULT_ELEMENTS);

        // When
        final Response response = RestApiTestUtil.executeOperationChainChunked(new OperationChain.Builder()
                .first(new GetAllElements<>())
                .then(new CountGroups())
                .build());

        // Then
        final List<GroupCounts> results = readChunkedResults(response, new GenericType<ChunkedInput<GroupCounts>>() {
        });
        assertEquals(1, results.size());
        final GroupCounts groupCounts = results.get(0);
        verifyGroupCounts(groupCounts);
    }

    @Test
    public void shouldReturnNoChunkedElementsWhenNoElementsInGraph() throws IOException {
        // When
        final Response response = RestApiTestUtil.executeOperationChainChunked(new OperationChain<>(new GetAllElements<>()));

        // Then
        final List<Element> results = readChunkedElements(response);
        assertEquals(0, results.size());
    }

    @Test
    public void shouldThrowErrorOnAddElements() throws IOException {
        // Given
        RestApiTestUtil.addElements(DEFAULT_ELEMENTS);

        // When
        final Response response = RestApiTestUtil.executeOperationChain(new OperationChain.Builder()
                .first(new AddElements(Collections.singleton(new Entity("wrong_group", "object"))))
                .build());

        System.out.println(response.readEntity(String.class));

        assertEquals(500, response.getStatus());
    }

    @Test
    public void shouldThrowErrorOnEmptyOperationChain() throws IOException {
        // When
        final Response response = RestApiTestUtil.executeOperationChain(new OperationChain());

        assertEquals(500, response.getStatus());
    }


    private List<Element> readChunkedElements(final Response response) {
        return readChunkedResults(response, new GenericType<ChunkedInput<Element>>() {
        });
    }

    private <T> List<T> readChunkedResults(final Response response, final GenericType<ChunkedInput<T>> genericType) {
        final ChunkedInput<T> input = response.readEntity(genericType);

        final List<T> results = new ArrayList<>();
        for (T result = input.read(); result != null; result = input.read()) {
            results.add(result);
        }
        return results;
    }

    private void verifyGroupCounts(final GroupCounts groupCounts) {
        assertEquals(2, (int) groupCounts.getEntityGroups()
                .get(TestGroups.ENTITY));
        assertEquals(1, (int) groupCounts.getEdgeGroups().get(TestGroups.EDGE));
        assertFalse(groupCounts.isLimitHit());
    }
}
