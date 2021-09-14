/*
 * Copyright 2019-2021 Crown Copyright
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
package uk.gov.gchq.gaffer.integration.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.If;
import uk.gov.gchq.gaffer.operation.impl.Map;
import uk.gov.gchq.gaffer.operation.util.Conditional;
import uk.gov.gchq.koryphe.impl.function.ToList;
import uk.gov.gchq.koryphe.impl.function.ToLong;
import uk.gov.gchq.koryphe.impl.function.ToLowerCase;
import uk.gov.gchq.koryphe.impl.function.ToUpperCase;
import uk.gov.gchq.koryphe.impl.predicate.IsA;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class IfIT extends AbstractStoreIT {

    public static final String INPUT_CAMEL_CASE = "AbCd";

    @Test
    public void shouldRunThenOperationWhenConditionIsTrue() throws OperationException {
        // Given
        final If<Object, Object> ifOperation = new If<>();
        ifOperation.setInput(INPUT_CAMEL_CASE);
        ifOperation.setConditional(new Conditional(new IsA("java.lang.String")));
        ifOperation.setThen(new Map<>(Lists.newArrayList(new ToUpperCase(), new ToList())));
        ifOperation.setOtherwise(new Map<>(Lists.newArrayList(new ToLowerCase(), new ToList())));

        // When
        final Object output = graph.execute(ifOperation, getUser());

        // Then
        assertThat(output).isEqualTo(Lists.newArrayList(INPUT_CAMEL_CASE.toUpperCase()))
                .isInstanceOf(List.class);
    }

    @Test
    public void shouldRunOtherwiseOperationsWhenConditionIsFalse() throws OperationException {
        // Given
        final If<Object, Object> ifOperation = new If<>();
        ifOperation.setInput(INPUT_CAMEL_CASE);
        ifOperation.setConditional(new Conditional(new IsA("java.lang.Integer")));
        ifOperation.setThen(new Map<>(Lists.newArrayList(new ToUpperCase(), new ToList())));
        ifOperation.setOtherwise(new Map<>(Lists.newArrayList(new ToLowerCase(), new ToList())));

        // When
        final Object output = graph.execute(ifOperation, getUser());

        // Then
        assertThat(output).isEqualTo(Lists.newArrayList(INPUT_CAMEL_CASE.toLowerCase()))
                .isInstanceOf(List.class);
    }

    @Test
    public void shouldReturnOriginalInputWhenConditionIsFalseAndNoOtherwise() throws OperationException {
        // Given
        final If<Object, Object> ifOperation = new If<>();
        ifOperation.setInput(INPUT_CAMEL_CASE);
        ifOperation.setConditional(new Conditional(new IsA("java.lang.Integer")));
        ifOperation.setThen(new Map<>(Lists.newArrayList(new ToLong(), new ToList())));

        // When
        final Object output = graph.execute(ifOperation, getUser());

        // Then
        assertThat(output).isEqualTo(INPUT_CAMEL_CASE)
                .isInstanceOf(String.class);
    }

    @Test
    public void shouldDoOtherwiseWhenConditionIsFalseAndNoThenOperation() throws OperationException {
        // Given
        final If<Object, Object> ifOperation = new If<>();
        ifOperation.setInput(INPUT_CAMEL_CASE);
        ifOperation.setConditional(new Conditional(new IsA("java.lang.Integer")));
        ifOperation.setOtherwise(new Map<>(Lists.newArrayList(new ToLowerCase(), new ToList())));

        // When
        final Object output = graph.execute(ifOperation, getUser());

        // Then
        assertThat(output).isEqualTo(Lists.newArrayList(INPUT_CAMEL_CASE.toLowerCase()))
                .isInstanceOf(List.class);
    }

    @Test
    public void shouldReturnOriginalInputWhenConditionIsTrueAndNoThen() throws OperationException {
        // Given
        final If<Object, Object> ifOperation = new If<>();
        ifOperation.setInput(INPUT_CAMEL_CASE);
        ifOperation.setConditional(new Conditional(new IsA("java.lang.String")));
        ifOperation.setOtherwise(new Map<>(Lists.newArrayList(new ToLong(), new ToList())));

        // When
        final Object output = graph.execute(ifOperation, getUser());

        // Then
        assertThat(output).isEqualTo(INPUT_CAMEL_CASE)
                .isInstanceOf(String.class);
    }
}
