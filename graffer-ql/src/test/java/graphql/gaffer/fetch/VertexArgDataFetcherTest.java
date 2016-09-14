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

package graphql.gaffer.fetch;

import gaffer.graphql.fetch.VertexArgDataFetcher;
import graphql.schema.DataFetchingEnvironment;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class VertexArgDataFetcherTest {
    private static final String KEY = "MyKey";

    @Test
    public void test() {
        /**
         * Given
         */
        final DataFetchingEnvironment env = mock(DataFetchingEnvironment.class);
        final String expectedResult = "My Answer";
        when(env.getArgument(KEY)).thenReturn(expectedResult);

        /**
         * When
         */
        final Object result = new VertexArgDataFetcher(KEY).get(env);

        /**
         * Then
         */
        assertEquals(expectedResult, result);
    }
}
