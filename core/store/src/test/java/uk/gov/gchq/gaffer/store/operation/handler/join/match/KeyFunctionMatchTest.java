/*
 * Copyright 2019 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.handler.join.match;

import com.google.common.collect.Lists;
import org.junit.Test;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ExtractId;
import uk.gov.gchq.gaffer.data.element.function.ExtractProperty;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.types.TypeSubTypeValue;
import uk.gov.gchq.koryphe.function.FunctionComposite;
import uk.gov.gchq.koryphe.impl.function.CallMethod;
import uk.gov.gchq.koryphe.impl.function.DivideBy;
import uk.gov.gchq.koryphe.impl.function.FirstItem;
import uk.gov.gchq.koryphe.impl.function.ToInteger;
import uk.gov.gchq.koryphe.impl.function.ToLong;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class KeyFunctionMatchTest {

    private static final String TEST_ENTITY_GROUP = "testEntity1";
    private static final String TEST_ENTITY_GROUP_2 = "testEntity2";
    private static final String TEST_EDGE_GROUP = "testEdge";
    private static final String PROP_1 = "prop1";
    private static final String PROP_2 = "prop2";

    @Test
    public void shouldJsonSerialiseWithNoKeyFunctions() throws SerialisationException {
        // given
        String json = "{\n" +
                "   \"class\": \"uk.gov.gchq.gaffer.store.operation.handler.join.match.KeyFunctionMatch\"\n" +
                "}";

        // when
        KeyFunctionMatch match = new KeyFunctionMatch();

        // then
        assertEquals(match, JSONSerialiser.deserialise(json, KeyFunctionMatch.class));
    }

    @Test
    public void shouldAddDefaultIdentityFunctionToJson() throws SerialisationException {
        // given
        KeyFunctionMatch match = new KeyFunctionMatch();

        // when / then
        String expected = "{\n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.store.operation.handler.join.match.KeyFunctionMatch\",\n" +
                "  \"firstKeyFunction\" : {\n" +
                "    \"class\" : \"uk.gov.gchq.koryphe.impl.function.Identity\"\n" +
                "  },\n" +
                "  \"secondKeyFunction\" : {\n" +
                "    \"class\" : \"uk.gov.gchq.koryphe.impl.function.Identity\"\n" +
                "  }\n" +
                "}";
        assertEquals(expected, new String(JSONSerialiser.serialise(match, true)));
    }

    @Test
    public void shouldJsonSerialiseAndDeserialiseWithKeyFunctions() throws SerialisationException {
        // given
        KeyFunctionMatch match = new KeyFunctionMatch.Builder()
                .firstKeyFunction(new FunctionComposite(Lists.newArrayList(new DivideBy(20), new FirstItem())))
                .secondKeyFunction(new ExtractProperty("count"))
                .build();

        // when / then
        String expected = "{\n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.store.operation.handler.join.match.KeyFunctionMatch\",\n" +
                "  \"firstKeyFunction\" : {\n" +
                "    \"class\" : \"uk.gov.gchq.koryphe.function.FunctionComposite\",\n" +
                "    \"functions\" : [ {\n" +
                "      \"class\" : \"uk.gov.gchq.koryphe.impl.function.DivideBy\",\n" +
                "      \"by\" : 20\n" +
                "    }, {\n" +
                "      \"class\" : \"uk.gov.gchq.koryphe.impl.function.FirstItem\"\n" +
                "    } ]\n" +
                "  },\n" +
                "  \"secondKeyFunction\" : {\n" +
                "    \"class\" : \"uk.gov.gchq.gaffer.data.element.function.ExtractProperty\",\n" +
                "    \"name\" : \"count\"\n" +
                "  }\n" +
                "}";

        assertEquals(expected, new String(JSONSerialiser.serialise(match, true)));
        assertEquals(match, JSONSerialiser.deserialise(expected, KeyFunctionMatch.class));
    }

    @Test
    public void shouldJsonSerialiseAndDeserialiseWithSingleFirstKeyFunction() throws SerialisationException {
        // given
        KeyFunctionMatch match = new KeyFunctionMatch.Builder().firstKeyFunction(new ExtractProperty("count")).build();

        // when / then
        String json = "{\n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.store.operation.handler.join.match.KeyFunctionMatch\",\n" +
                "  \"firstKeyFunction\" : {\n" +
                "    \"class\" : \"uk.gov.gchq.gaffer.data.element.function.ExtractProperty\",\n" +
                "    \"name\" : \"count\"\n" +
                "  }\n" +
                "}";

        assertEquals(match, JSONSerialiser.deserialise(json, KeyFunctionMatch.class));

        // when / then

        String jsonWithIdentity = "{\n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.store.operation.handler.join.match.KeyFunctionMatch\",\n" +
                "  \"firstKeyFunction\" : {\n" +
                "    \"class\" : \"uk.gov.gchq.gaffer.data.element.function.ExtractProperty\",\n" +
                "    \"name\" : \"count\"\n" +
                "  },\n" +
                "  \"secondKeyFunction\" : {\n" +
                "    \"class\" : \"uk.gov.gchq.koryphe.impl.function.Identity\"\n" +
                "  }\n" +
                "}";

        assertEquals(jsonWithIdentity, new String(JSONSerialiser.serialise(match, true)));

    }

    @Test
    public void shouldJsonSerialiseAndDeserialiseWithSingleRightKeyFunction() throws SerialisationException {
        // given
        KeyFunctionMatch match = new KeyFunctionMatch.Builder().secondKeyFunction(new ExtractProperty("count")).build();

        // when / then
        String json = "{\n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.store.operation.handler.join.match.KeyFunctionMatch\",\n" +
                "  \"secondKeyFunction\" : {\n" +
                "    \"class\" : \"uk.gov.gchq.gaffer.data.element.function.ExtractProperty\",\n" +
                "    \"name\" : \"count\"\n" +
                "  }\n" +
                "}";

        assertEquals(match, JSONSerialiser.deserialise(json, KeyFunctionMatch.class));

        // when / then

        String jsonWithIdentity = "{\n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.store.operation.handler.join.match.KeyFunctionMatch\",\n" +
                "  \"firstKeyFunction\" : {\n" +
                "    \"class\" : \"uk.gov.gchq.koryphe.impl.function.Identity\"\n" +
                "  },\n" +
                "  \"secondKeyFunction\" : {\n" +
                "    \"class\" : \"uk.gov.gchq.gaffer.data.element.function.ExtractProperty\",\n" +
                "    \"name\" : \"count\"\n" +
                "  }\n" +
                "}";

        assertEquals(jsonWithIdentity, new String(JSONSerialiser.serialise(match, true)));
    }

    @Test
    public void shouldThrowExceptionIfKeyFunctionsAreSetToNull() {
        // given
        Integer testValue = 3;
        List<Integer> testList = new ArrayList<>();

        // when
        KeyFunctionMatch match = new KeyFunctionMatch.Builder()
                .firstKeyFunction(null)
                .secondKeyFunction(null)
                .build();

        match.init(testList);

        // then

        try {
            match.matching(testValue);
        } catch (IllegalArgumentException e) {
            assertEquals("Key functions for left and right input cannot be null", e.getMessage());
        }
    }

    @Test
    public void shouldMatchEqualObjectsIfNoKeyFunctionIsSpecified() {
        // given
        Integer testValue = 3;
        List<Integer> testList = Lists.newArrayList(1, 2, 3, 4, 3);

        // when
        KeyFunctionMatch match = new KeyFunctionMatch();
        match.init(testList);

        // then
        List<Integer> expected = Lists.newArrayList(3, 3);
        assertEquals(expected, match.matching(testValue));
    }

    @Test
    public void shouldMatchObjectsBasedOnKeyFunctions() {
        // given
        TypeSubTypeValue testValue = new TypeSubTypeValue("myType", "mySubType", "30");
        List<Long> testList = Lists.newArrayList(100L, 200L, 300L, 400L);

        // when
        KeyFunctionMatch match = new KeyFunctionMatch.Builder()
                .firstKeyFunction(
                        new FunctionComposite(
                                Lists.newArrayList(new CallMethod("getValue"), new ToInteger())))
                .secondKeyFunction(new FunctionComposite(Lists.newArrayList(new ToInteger(), new DivideBy(10), new FirstItem<>())))
                .build();

        match.init(testList);

        // then
        List<Long> expected = Lists.newArrayList(300L);
        assertEquals(expected, match.matching(testValue));

    }

    @Test
    public void shouldOutputEmptyListWhenNoMatchesAreFound() {
        // given
        Integer testValue = 3;
        List<Integer> testList = Lists.newArrayList(1, 2, 5, 4, 8);

        // when
        KeyFunctionMatch match = new KeyFunctionMatch();

        match.init(testList);

        // then
        List<Integer> expected = Lists.newArrayList();
        assertEquals(expected, match.matching(testValue));
    }

    @Test
    public void shouldOutputEmptyListWhenEmptyListIsSupplied() {
        // given
        Integer testValue = 3;
        List<Integer> testList = Lists.newArrayList();

        // when
        KeyFunctionMatch match = new KeyFunctionMatch();
        match.init(testList);

        // then
        List<Integer> expected = Lists.newArrayList();
        assertEquals(expected, match.matching(testValue));
    }

    @Test
    public void shouldThrowExceptionFromFunctionIfInputIsInvalid() {
        // given
        // Performing a FirstItem on null should throw IllegalArgumentException
        List<Long> testList = Lists.newArrayList(100L, 200L, 300L, null);

        // when
        KeyFunctionMatch match = new KeyFunctionMatch.Builder()
                .firstKeyFunction(new FunctionComposite(
                        Lists.newArrayList(new CallMethod("getValue"), new ToInteger())))
                .secondKeyFunction(new FunctionComposite(Lists.newArrayList(new ToInteger(), new DivideBy(10), new FirstItem<>())))
                .build();


        // then
        try {
            match.init(testList);
        } catch (final IllegalArgumentException e) {
            // copied from docs of FirstItem
            assertEquals("Input cannot be null", e.getMessage());
        }
    }

    @Test
    public void shouldAllowNullValuesIfValid() {
        // given
        List<Integer> testList = Lists.newArrayList(1, null, 5, 4, 8);

        // when
        KeyFunctionMatch match = new KeyFunctionMatch();
        match.init(testList);

        // then
        List<Integer> expected = Lists.newArrayList((Integer) null);
        assertEquals(expected, match.matching(null));
    }

    @Test
    public void shouldAllowNullValuesInList() {
        // given
        Integer testItem = 4;
        List<Integer> testList = Lists.newArrayList(1, null, 5, 4, 8);

        // when
        KeyFunctionMatch match = new KeyFunctionMatch();
        match.init(testList);

        // then
        List<Integer> expected = Lists.newArrayList(4);
        assertEquals(expected, match.matching(testItem));
    }

    @Test
    public void shouldThrowExceptionIfListIsNull() {
        // given
        KeyFunctionMatch match = new KeyFunctionMatch.Builder().build();

        // when / then

        try {
            match.init(null);
        } catch (IllegalArgumentException e) {
            assertEquals("Iterable of match candidates cannot be null", e.getMessage());
        }
    }

    @Test
    public void shouldMatchElementsOfTheSameGroupBasedOnKeyFunctions() {
        // given
        Entity testItem = new Entity.Builder().group(TEST_ENTITY_GROUP)
                .vertex("test")
                .property(PROP_1, 3L)
                .build();

        List<Entity> testList = Lists.newArrayList(
                new Entity.Builder()
                        .group(TEST_ENTITY_GROUP)
                        .vertex("test2")
                        .property(PROP_1, 2L)
                        .build(),
                new Entity.Builder()
                        .group(TEST_ENTITY_GROUP)
                        .vertex("test3")
                        .property(PROP_1, 3L)
                        .build(),
                new Entity.Builder()
                        .group(TEST_ENTITY_GROUP)
                        .vertex("test1")
                        .property(PROP_1, 4L)
                        .build(),
                new Entity.Builder()
                        .group(TEST_ENTITY_GROUP)
                        .vertex("test3")
                        .property(PROP_1, 3L)
                        .build()
        );

        // when
        KeyFunctionMatch match = new KeyFunctionMatch.Builder()
                .firstKeyFunction(new ExtractProperty(PROP_1))
                .secondKeyFunction(new ExtractProperty(PROP_1))
                .build();

        match.init(testList);

        // then
        ArrayList<Entity> expected = Lists.newArrayList(
                new Entity.Builder()
                        .group(TEST_ENTITY_GROUP)
                        .vertex("test3")
                        .property(PROP_1, 3L)
                        .build(),
                new Entity.Builder()
                        .group(TEST_ENTITY_GROUP)
                        .vertex("test3")
                        .property(PROP_1, 3L)
                        .build());

        assertEquals(expected, match.matching(testItem));
    }

    @Test
    public void shouldMatchElementsOfDifferentGroupsBasedOnKeyFunctions() {
        // given
        Entity testItem = new Entity.Builder().group(TEST_ENTITY_GROUP)
                .vertex("test")
                .property(PROP_1, 2L)
                .build();

        List<Entity> testList = Lists.newArrayList(
                new Entity.Builder()
                        .group(TEST_ENTITY_GROUP_2)
                        .vertex("test2")
                        .property(PROP_2, 2)
                        .build(),
                new Entity.Builder()
                        .group(TEST_ENTITY_GROUP)
                        .vertex("test3")
                        .property(PROP_1, 3L)
                        .build(),
                new Entity.Builder()
                        .group(TEST_ENTITY_GROUP)
                        .vertex("test1")
                        .property(PROP_1, 4L)
                        .build(),
                new Entity.Builder()
                        .group(TEST_ENTITY_GROUP_2)
                        .vertex("test3")
                        .property(PROP_2, 2)
                        .build()
        );

        // when
        KeyFunctionMatch match = new KeyFunctionMatch.Builder()
                .firstKeyFunction(new ExtractProperty(PROP_1))
                .secondKeyFunction(new FunctionComposite(Lists.newArrayList(new ExtractProperty(PROP_2), new ToLong())))
                .build();

        match.init(testList);

        // then
        ArrayList<Entity> expected = Lists.newArrayList(
                new Entity.Builder()
                        .group(TEST_ENTITY_GROUP_2)
                        .vertex("test2")
                        .property(PROP_2, 2)
                        .build(),
                new Entity.Builder()
                        .group(TEST_ENTITY_GROUP_2)
                        .vertex("test3")
                        .property(PROP_2, 2)
                        .build());

        assertEquals(expected, match.matching(testItem));
    }

    @Test
    public void shouldMatchElementsOfDifferentClassesBasedOnKeyFunctions() {
        // given

        Edge testItem = new Edge.Builder().group(TEST_EDGE_GROUP)
                .source("test1")
                .dest("test4")
                .directed(true)
                .property(PROP_1, 2L)
                .build();

        List<Entity> testList = Lists.newArrayList(
                new Entity.Builder()
                        .group(TEST_ENTITY_GROUP_2)
                        .vertex("test2")
                        .property(PROP_2, 2)
                        .build(),
                new Entity.Builder()
                        .group(TEST_ENTITY_GROUP)
                        .vertex("test3")
                        .property(PROP_1, 3L)
                        .build(),
                new Entity.Builder()
                        .group(TEST_ENTITY_GROUP)
                        .vertex("test1")
                        .property(PROP_1, 4L)
                        .build(),
                new Entity.Builder()
                        .group(TEST_ENTITY_GROUP_2)
                        .vertex("test3")
                        .property(PROP_2, 2)
                        .build()
        );

        // when

        KeyFunctionMatch match = new KeyFunctionMatch.Builder()
                .firstKeyFunction(new ExtractId(IdentifierType.SOURCE))
                .secondKeyFunction(new ExtractId(IdentifierType.VERTEX))
                .build();

        match.init(testList);

        // then
        ArrayList<Entity> expected = Lists.newArrayList(new Entity.Builder()
                .group(TEST_ENTITY_GROUP)
                .vertex("test1")
                .property(PROP_1, 4L)
                .build());

        assertEquals(expected, match.matching(testItem));
    }
}
