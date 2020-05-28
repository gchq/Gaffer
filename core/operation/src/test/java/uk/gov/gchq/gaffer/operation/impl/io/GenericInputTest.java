/*
 * Copyright 2017-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.impl.io;

import com.google.common.collect.Lists;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import uk.gov.gchq.gaffer.JSONSerialisationTest;
import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.operation.io.GenericInput;
import uk.gov.gchq.gaffer.types.TypeValue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class GenericInputTest extends JSONSerialisationTest<GenericInput> {

    @ParameterizedTest
    @MethodSource("instancesToTest")
    public void shouldHandleGenericInputType(String description, Object inputData, String expectedJson) {
        // Given
        final GenericInput input = new GenericInputImpl(inputData);

        // When / Then
        final byte[] json = toJson(input);
        JsonAssert.assertEquals(expectedJson.getBytes(), json);

        // When / Then
        final GenericInput inputFromJson = fromJson(json);
        assertInputEquals(input.getInput(), inputFromJson.getInput());

        // When / Then
        final byte[] json2 = toJson(inputFromJson);
        JsonAssert.assertEquals(expectedJson.getBytes(), json2);

        // When / Then
        final GenericInput inputFromJson2 = fromJson(json2);
        assertInputEquals(input.getInput(), inputFromJson2.getInput());
    }

    private static Stream<Arguments> instancesToTest() {
        return Stream.of(
                // Single values
                Arguments.of("Long", 1L,
                        "{\"class\":\"uk.gov.gchq.gaffer.operation.impl.io.GenericInputImpl\",\"input\":[\"java.lang.Long\",1]}"),
                Arguments.of("Integer", 1,
                        "{\"class\":\"uk.gov.gchq.gaffer.operation.impl.io.GenericInputImpl\",\"input\":1}"),
                Arguments.of("Date", new Date(0),
                        "{\"class\":\"uk.gov.gchq.gaffer.operation.impl.io.GenericInputImpl\",\"input\":[\"java.util.Date\",0]}"),
                Arguments.of("TypeValue", new TypeValue("type", "value"),
                        "{\"class\":\"uk.gov.gchq.gaffer.operation.impl.io.GenericInputImpl\",\"input\":{\"class\":\"uk.gov.gchq.gaffer.types.TypeValue\",\"type\":\"type\",\"value\":\"value\"}}"),

                // Array of values
                Arguments.of("Long array (size 1)", new Long[] {1L},
                        "{\"class\":\"uk.gov.gchq.gaffer.operation.impl.io.GenericInputImpl\",\"input\":[[\"java.lang.Long\",1]]}"),
                Arguments.of("Integer array (size 1)", new Integer[] {1},
                        "{\"class\":\"uk.gov.gchq.gaffer.operation.impl.io.GenericInputImpl\",\"input\":[1]}"),
                Arguments.of("Date array (size 1)", new Date[] {new Date(0)},
                        "{\"class\":\"uk.gov.gchq.gaffer.operation.impl.io.GenericInputImpl\",\"input\":[[\"java.util.Date\",0]]}"),
                Arguments.of("TypeValue array (size 1)", new TypeValue[] {new TypeValue("type1", "value1")},
                        "{\"class\":\"uk.gov.gchq.gaffer.operation.impl.io.GenericInputImpl\",\"input\":[{\"class\":\"uk.gov.gchq.gaffer.types.TypeValue\",\"type\":\"type1\",\"value\":\"value1\"}]}"),

                Arguments.of("Long array (size 2)", new Long[] {1L, 2L},
                        "{\"class\":\"uk.gov.gchq.gaffer.operation.impl.io.GenericInputImpl\",\"input\":[[\"java.lang.Long\",1],[\"java.lang.Long\",2]]}"),
                Arguments.of("Integer array (size 2)", new Integer[] {1, 2},
                        "{\"class\":\"uk.gov.gchq.gaffer.operation.impl.io.GenericInputImpl\",\"input\":[1,2]}"),
                Arguments.of("Date array (size 2)", new Date[] {new Date(0), new Date(1)},
                        "{\"class\":\"uk.gov.gchq.gaffer.operation.impl.io.GenericInputImpl\",\"input\":[[\"java.util.Date\",0],[\"java.util.Date\",1]]}"),
                Arguments.of("TypeValue array (size 2)", new TypeValue[] {new TypeValue("type1", "value1"), new TypeValue("type2", "value2")},
                        "{\"class\":\"uk.gov.gchq.gaffer.operation.impl.io.GenericInputImpl\",\"input\":[{\"class\":\"uk.gov.gchq.gaffer.types.TypeValue\",\"type\":\"type1\",\"value\":\"value1\"},{\"class\":\"uk.gov.gchq.gaffer.types.TypeValue\",\"type\":\"type2\",\"value\":\"value2\"}]}"),
                Arguments.of("Long array (size 3)", new Long[] {1L, 2L, 3L},
                        "{\"class\":\"uk.gov.gchq.gaffer.operation.impl.io.GenericInputImpl\",\"input\":[[\"java.lang.Long\",1],[\"java.lang.Long\",2],[\"java.lang.Long\",3]]}"),
                Arguments.of("Integer array (size 3)", new Integer[] {1, 2, 3},
                        "{\"class\":\"uk.gov.gchq.gaffer.operation.impl.io.GenericInputImpl\",\"input\":[1,2,3]}"),
                Arguments.of("Date array (size 3)", new Date[] {new Date(0), new Date(1), new Date(2)},
                        "{\"class\":\"uk.gov.gchq.gaffer.operation.impl.io.GenericInputImpl\",\"input\":[[\"java.util.Date\",0],[\"java.util.Date\",1],[\"java.util.Date\",2]]}"),
                Arguments.of("TypeValue array (size 3)", new TypeValue[] {new TypeValue("type1", "value1"), new TypeValue("type2", "value2"), new TypeValue("type3", "value3")},
                        "{\"class\":\"uk.gov.gchq.gaffer.operation.impl.io.GenericInputImpl\",\"input\":[{\"class\":\"uk.gov.gchq.gaffer.types.TypeValue\",\"type\":\"type1\",\"value\":\"value1\"},{\"class\":\"uk.gov.gchq.gaffer.types.TypeValue\",\"type\":\"type2\",\"value\":\"value2\"},{\"class\":\"uk.gov.gchq.gaffer.types.TypeValue\",\"type\":\"type3\",\"value\":\"value3\"}]}")
        );
    }

    @Override
    protected GenericInput getTestObject() {
        return new GenericInputImpl();
    }

    private void assertInputEquals(final Object expected, final Object actual) {
        final List<Object> expectedList = getAsList(expected);
        final List<Object> actualList = getAsList(actual);

        if (expectedList.size() > 1 && actualList.size() > 1) {
            if (expectedList.size() != actualList.size()) {
                fail("Expected and actual objects must be the same length.");
            }

            zip(expectedList, actualList).forEach(pair -> assertEquals(pair.getFirst(), pair.getSecond()));

        } else {
            assertEquals(expectedList, actualList);
        }
    }

    private <A, B> Stream<Pair<A, B>> zip(final List<A> as, final List<B> bs) {
        return IntStream.range(0, Math.min(as.size(), bs.size()))
                .mapToObj(i -> new Pair<>(as.get(i), bs.get(i)));
    }

    private List<Object> getAsList(final Object obj) {
        if (obj.getClass().isArray()) {
            final List<Object> objList = new ArrayList<>();
            Collections.addAll(objList, ((Object[]) obj));
            return objList;
        } else if (obj instanceof Collection) {
            return Lists.newArrayList((Collection) obj);
        } else {
            return Lists.newArrayList(obj);
        }
    }
}
