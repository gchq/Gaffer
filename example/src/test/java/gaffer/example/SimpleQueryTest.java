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

package gaffer.example;

import com.google.common.collect.Lists;
import gaffer.example.data.Viewing;
import gaffer.operation.OperationException;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SimpleQueryTest {
    @Test
    public void shouldReturnExpectedEdgesForSimpleQuery() throws OperationException {
        // Given
        final SimpleQuery query = new SimpleQuery();
        final List<Viewing> expectedResults = Arrays.asList(
                new Viewing("filmA", "user01", 1401000000000L),
                new Viewing("filmA", "user02", 1401000000000L),
                new Viewing("filmA", "user03", 1408000000000L)
        );

        // When
        final Iterable<Viewing> resultsItr = query.run();

        // Then
        assertEquals(expectedResults, Lists.newArrayList(resultsItr));
    }
}