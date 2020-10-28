/*
 * Copyright 2020 Crown Copyright
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
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class OperationDetailTest {
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
}
