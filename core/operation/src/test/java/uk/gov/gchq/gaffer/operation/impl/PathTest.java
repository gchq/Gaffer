/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;

import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;
import static uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser.createDefaultMapper;

public class PathTest extends OperationTest<Path> {

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final Path path = new Path.Builder()
                .input(new EntitySeed("1"), new EntitySeed("2"))
                .operations(new GetElements())
                .build();

        // Then
        assertThat(path.getInput(), is(notNullValue()));
        assertThat(path.getInput(), iterableWithSize(2));
        assertThat(path.getOperations(), iterableWithSize(1));
        assertThat(path.getInput(), containsInAnyOrder(new EntitySeed("1"), new EntitySeed("2")));
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final List<EntitySeed> input = Lists.newArrayList(new EntitySeed("1"), new EntitySeed("2"));
        final List<GetElements> operations = Lists.newArrayList(new GetElements());
        final Path path = new Path.Builder()
                .input(input)
                .operations(operations)
                .build();

        // When
        final Path clone = path.shallowClone();

        // Then
        assertNotSame(path, clone);
        assertEquals(input, Lists.newArrayList(clone.getInput()));
        assertEquals(operations, clone.getOperations());
    }

    @Test
    public void shouldSerialiseToJson() throws JsonProcessingException {
        // Given
        final Path path = new Path.Builder()
                .input(new EntitySeed("1"))
                .operations(new GetElements.Builder()
                        .input(new EntitySeed("1"))
                        .view(new View.Builder()
                                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                        .properties(TestPropertyNames.COUNT)
                                        .build())
                                .build())
                        .build())
                .build();

        // When
        final ObjectMapper mapper = createDefaultMapper();
        final String jsonString = mapper.writeValueAsString(path);

        // Then
        System.out.println(jsonString);
    }

    @Override
    protected Path getTestObject() {
        return new Path();
    }
}