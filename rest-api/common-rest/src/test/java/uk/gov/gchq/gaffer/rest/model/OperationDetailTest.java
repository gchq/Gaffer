/*
 * Copyright 2020-2021 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.model;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.DiscardOutput;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.koryphe.util.EqualityTest;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OperationDetailTest extends EqualityTest<OperationDetail> {
    @Test
    public void shouldUseSummaryAnnotationForSummary() {
        // Given
        OperationDetail operationDetail = new OperationDetail(GetElements.class, null, new GetElements());

        // When
        String summary = operationDetail.getSummary();

        // Then
        assertEquals("Gets elements related to provided seeds", summary);
    }

    @Test
    public void shouldGetFullyQualifiedOutputType() {
        // Given
        OperationDetail operationDetail = new OperationDetail(GetElements.class, null, new GetElements());

        // When
        String outputClassName = operationDetail.getOutputClassName();

        // Then
        assertEquals("uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable<uk.gov.gchq.gaffer.data.element.Element>", outputClassName);
    }

    @Test
    public void shouldShowOperationFields() {
        // Given
        OperationDetail operationDetail = new OperationDetail(NamedOperation.class, null, new NamedOperation<>());

        // When
        List<String> fieldNames = operationDetail.getFields().stream().map(OperationField::getName).collect(Collectors.toList());

        // Then
        ArrayList<String> expected = Lists.newArrayList("input", "options", "operationName", "parameters");
        assertEquals(expected, fieldNames);
    }

    @Test
    public void shouldOutputWhetherAFieldIsRequired() {
        // Given
        OperationDetail operationDetail = new OperationDetail(NamedOperation.class, null, new NamedOperation<>());

        // When
        operationDetail.getFields().forEach(field -> {
            if (field.getName().equals("operationName")) {
                assertTrue(field.isRequired());
            } else {
                assertFalse(field.isRequired());
            }
        });
    }

    @Override
    protected OperationDetail getInstance() {
        return new OperationDetail(
                GetElements.class,
                Sets.newHashSet(GetElements.class, GetAdjacentIds.class),
                new GetElements()
        );
    }

    @Override
    protected Iterable<OperationDetail> getDifferentInstancesOrNull() {
        return Lists.newArrayList(
                new OperationDetail(
                        GetAdjacentIds.class,
                        Sets.newHashSet(GetElements.class, GetAdjacentIds.class),
                        new GetElements()
                ),
                new OperationDetail(
                        GetAdjacentIds.class,
                        Sets.newHashSet(GetElements.class, GetAdjacentIds.class),
                        new GetElements()
                ),
                new OperationDetail(
                        GetAdjacentIds.class,
                        Sets.newHashSet(DiscardOutput.class, GetElements.class, GetAdjacentIds.class),
                        new GetElements()
                ),
                new OperationDetail(
                        GetAdjacentIds.class,
                        Sets.newHashSet(GetElements.class, GetAdjacentIds.class),
                        new GetElements.Builder()
                                .input(new EntitySeed("test"))
                                .build()
                ),
                new OperationDetail(
                        GetAdjacentIds.class,
                        null,
                        new GetElements()
                ),
                new OperationDetail(
                        GetAdjacentIds.class,
                        null,
                        null
                )

        );
    }
}
