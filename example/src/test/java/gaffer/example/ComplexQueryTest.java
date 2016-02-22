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
import gaffer.data.element.Entity;
import gaffer.example.data.schema.Group;
import gaffer.example.data.schema.Property;
import gaffer.example.data.schema.TransientProperty;
import gaffer.operation.OperationException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ComplexQueryTest {
    @Test
    public void shouldReturnExpectedEdgesForComplexQuery() throws OperationException {
        // Given
        final ComplexQuery query = new ComplexQuery();
        final List<Entity> expectedResults = new ArrayList<>();
        final Entity entity = new Entity(Group.REVIEW, "filmA");
        entity.putProperty(Property.USER_ID, "user01,user03");
        entity.putProperty(Property.RATING, 100L);
        entity.putProperty(Property.COUNT, 2);
        entity.putProperty(TransientProperty.FIVE_STAR_RATING, 2.5F);
        expectedResults.add(entity);

        // When
        final Iterable<Entity> resultsItr = query.run();

        // Then
        final List<Entity> results = Lists.newArrayList(resultsItr);
        assertEquals(expectedResults.size(), results.size());
        for (int i = 0; i < expectedResults.size(); i++) {
            assertTrue(expectedResults.get(i).deepEquals(results.get(i)));
        }
    }
}