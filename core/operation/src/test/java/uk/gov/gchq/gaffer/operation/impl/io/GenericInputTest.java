/*
 * Copyright 2017-2018 Crown Copyright
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import uk.gov.gchq.gaffer.JSONSerialisationTest;
import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.io.GenericInput;
import uk.gov.gchq.gaffer.types.TypeValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class GenericInputTest extends JSONSerialisationTest<GenericInput> {

    @Parameters(name = "{0}")
    public static Collection<Object[]> instancesToTest() {
        return Arrays.asList(new Object[][]{
                // Single values
                {"Long", 1L,
                        "{\"class\":\"uk.gov.gchq.gaffer.operation.impl.io.GenericInputImpl\",\"input\":[\"java.lang.Long\",1]}"},
                {"Integer", 1,
                        "{\"class\":\"uk.gov.gchq.gaffer.operation.impl.io.GenericInputImpl\",\"input\":1}"},
                {"Date", new Date(0),
                        "{\"class\":\"uk.gov.gchq.gaffer.operation.impl.io.GenericInputImpl\",\"input\":[\"java.util.Date\",0]}"},
                {"TypeValue", new TypeValue("type", "value"),
                        "{\"class\":\"uk.gov.gchq.gaffer.operation.impl.io.GenericInputImpl\",\"input\":{\"class\":\"uk.gov.gchq.gaffer.types.TypeValue\",\"type\":\"type\",\"value\":\"value\"}}"},

                // Array of values
                {"Long array (size 1)", new Long[]{1L},
                        "{\"class\":\"uk.gov.gchq.gaffer.operation.impl.io.GenericInputImpl\",\"input\":[[\"java.lang.Long\",1]]}"},
                {"Integer array (size 1)", new Integer[]{1},
                        "{\"class\":\"uk.gov.gchq.gaffer.operation.impl.io.GenericInputImpl\",\"input\":[1]}"},
                {"Date array (size 1)", new Date[]{new Date(0)},
                        "{\"class\":\"uk.gov.gchq.gaffer.operation.impl.io.GenericInputImpl\",\"input\":[[\"java.util.Date\",0]]}"},
                {"TypeValue array (size 1)", new TypeValue[]{new TypeValue("type1", "value1")},
                        "{\"class\":\"uk.gov.gchq.gaffer.operation.impl.io.GenericInputImpl\",\"input\":[{\"class\":\"uk.gov.gchq.gaffer.types.TypeValue\",\"type\":\"type1\",\"value\":\"value1\"}]}"},

                {"Long array (size 2)", new Long[]{1L, 2L},
                        "{\"class\":\"uk.gov.gchq.gaffer.operation.impl.io.GenericInputImpl\",\"input\":[[\"java.lang.Long\",1],[\"java.lang.Long\",2]]}"},
                {"Integer array (size 2)", new Integer[]{1, 2},
                        "{\"class\":\"uk.gov.gchq.gaffer.operation.impl.io.GenericInputImpl\",\"input\":[1,2]}"},
                {"Date array (size 2)", new Date[]{new Date(0), new Date(1)},
                        "{\"class\":\"uk.gov.gchq.gaffer.operation.impl.io.GenericInputImpl\",\"input\":[[\"java.util.Date\",0],[\"java.util.Date\",1]]}"},
                {"TypeValue array (size 2)", new TypeValue[]{new TypeValue("type1", "value1"), new TypeValue("type2", "value2")},
                        "{\"class\":\"uk.gov.gchq.gaffer.operation.impl.io.GenericInputImpl\",\"input\":[{\"class\":\"uk.gov.gchq.gaffer.types.TypeValue\",\"type\":\"type1\",\"value\":\"value1\"},{\"class\":\"uk.gov.gchq.gaffer.types.TypeValue\",\"type\":\"type2\",\"value\":\"value2\"}]}"},

                {"Long array (size 3)", new Long[]{1L, 2L, 3L},
                        "{\"class\":\"uk.gov.gchq.gaffer.operation.impl.io.GenericInputImpl\",\"input\":[[\"java.lang.Long\",1],[\"java.lang.Long\",2],[\"java.lang.Long\",3]]}"},
                {"Integer array (size 3)", new Integer[]{1, 2, 3},
                        "{\"class\":\"uk.gov.gchq.gaffer.operation.impl.io.GenericInputImpl\",\"input\":[1,2,3]}"},
                {"Date array (size 3)", new Date[]{new Date(0), new Date(1), new Date(2)},
                        "{\"class\":\"uk.gov.gchq.gaffer.operation.impl.io.GenericInputImpl\",\"input\":[[\"java.util.Date\",0],[\"java.util.Date\",1],[\"java.util.Date\",2]]}"},
                {"TypeValue array (size 3)", new TypeValue[]{new TypeValue("type1", "value1"), new TypeValue("type2", "value2"), new TypeValue("type3", "value3")},
                        "{\"class\":\"uk.gov.gchq.gaffer.operation.impl.io.GenericInputImpl\",\"input\":[{\"class\":\"uk.gov.gchq.gaffer.types.TypeValue\",\"type\":\"type1\",\"value\":\"value1\"},{\"class\":\"uk.gov.gchq.gaffer.types.TypeValue\",\"type\":\"type2\",\"value\":\"value2\"},{\"class\":\"uk.gov.gchq.gaffer.types.TypeValue\",\"type\":\"type3\",\"value\":\"value3\"}]}"}
        });
    }

    private final String expectedJson;
    private final Object input;

    public GenericInputTest(final String description, final Object input, final String expectedJson) {
        this.input = input;
        this.expectedJson = expectedJson;
    }

    @Test
    public void shouldHandleGenericInputType() throws JsonProcessingException, SerialisationException {
        // Given
        final GenericInput input = getTestObject();

        // When / Then
        final byte[] json = toJson(input);
        JsonAssert.assertEquals(getExpectedJson(), json);

        // When / Then
        final GenericInput inputFromJson = fromJson(json);
        assertInputEquals(input.getInput(), inputFromJson.getInput());

        // When / Then
        final byte[] json2 = toJson(inputFromJson);
        JsonAssert.assertEquals(getExpectedJson(), json2);

        // When / Then
        final GenericInput inputFromJson2 = fromJson(json2);
        assertInputEquals(input.getInput(), inputFromJson2.getInput());
    }

    public byte[] getExpectedJson() {
        return expectedJson.getBytes();
    }

    @Override
    protected GenericInput getTestObject() {
        return new GenericInputImpl(input);
    }

    private void assertInputEquals(final Object expected, final Object actual) {
        final List<Object> expectedList = getAsList(expected);
        final List<Object> actualList = getAsList(actual);

        if (expectedList.size() > 1 && actualList.size() > 1) {
            if (expectedList.size() != actualList.size()) {
                fail("Expected and actual objects must be the same length.");
            }

            zip(expectedList, actualList).forEach(pair -> {
                assertEquals(pair.getFirst(), pair.getSecond());
            });

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
            for (int i = 0; i < ((Object[]) obj).length; i++) {
                objList.add(((Object[]) obj)[i]);
            }
            return objList;
        } else if (obj instanceof Collection) {
            return Lists.newArrayList((Collection) obj);
        } else {
            return Lists.newArrayList(obj);
        }
    }
}
