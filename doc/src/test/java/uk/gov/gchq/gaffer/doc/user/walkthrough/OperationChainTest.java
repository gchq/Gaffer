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

package uk.gov.gchq.gaffer.doc.user.walkthrough;

import com.google.common.collect.Lists;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Test;
import uk.gov.gchq.gaffer.operation.OperationException;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class OperationChainTest {
    @Test
    public void shouldReturnExpectedEdges() throws OperationException, IOException {
        // Given
        final OperationChains query = new OperationChains();

        // When
        final Iterable<? extends String> results = query.run();

        // Then
        verifyResults(results);
    }

    private void verifyResults(final Iterable<? extends String> resultsItr) {
        final String[] expectedResults = {
                "10,11,3",
                "11,10,1",
                "23,24,2",
                "28,27,1"
        };

        final List<String> results = Lists.newArrayList(resultsItr);
        assertEquals(expectedResults.length, results.size());
        assertThat(results, IsCollectionContaining.hasItems(expectedResults));
    }

}