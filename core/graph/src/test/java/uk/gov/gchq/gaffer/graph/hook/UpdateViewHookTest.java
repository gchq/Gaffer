/*
 * Copyright 2018-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.graph.hook;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.graph.OperationView;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.gaffer.user.User.Builder;
import uk.gov.gchq.koryphe.impl.predicate.Exists;
import uk.gov.gchq.koryphe.impl.predicate.IsIn;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.gchq.gaffer.commonutil.JsonAssert.assertEquals;

public class UpdateViewHookTest extends GraphHookTest<UpdateViewHook> {

    public static final String TEST_WITH_VALUE = "withTestValue";
    public static final String TEST_WITHOUT_VALUE = "withoutTestValue";
    public static final String TEST_EDGE = "testEdge";
    public static final String A = "A";
    public static final String B = "B";
    private OperationChain opChain;
    private View viewToMerge;
    private HashSet<String> userOpAuths = Sets.newHashSet();
    private HashSet<String> userDataAuths = Sets.newHashSet();
    private HashSet<String> opAuths = Sets.newHashSet();
    private HashSet<String> dataAuths = Sets.newHashSet();
    private HashSet<String> validAuths = Sets.newHashSet();
    private HashSet<String> userAuths = Sets.newHashSet();
    private Builder userBuilder;
    private UpdateViewHook updateViewHook;

    public UpdateViewHookTest() {
        super(UpdateViewHook.class);
    }

    @BeforeEach
    public void setUp() throws Exception {
        userAuths.clear();
        validAuths.clear();

        updateViewHook = new UpdateViewHook();
        viewToMerge = new View.Builder()
                .entity("white1", new ViewElementDefinition.Builder()
                        .preAggregationFilter(new ElementFilter.Builder()
                                .select("prop1")
                                .execute(new IsIn("value1", "value2"))
                                .build())
                        .build())
                .edge("testGroup", new ViewElementDefinition.Builder()
                        .preAggregationFilter(new ElementFilter.Builder()
                                .select("count")
                                .execute(new IsMoreThan(10))
                                .build())
                        .build())
                .build();

        userOpAuths.clear();
        userDataAuths.clear();
        opAuths.clear();
        dataAuths.clear();

        userBuilder = new Builder();
    }

    //***** preExecute TESTS *****//

    @Test
    public void shouldNotAddExtraGroupsToUsersView() {
        // Given
        opChain = new OperationChain.Builder()
                .first(new GetAllElements.Builder()
                        .view(new View.Builder()
                                .entity("entity1")
                                .edge("edge1")
                                .build())
                        .build())
                .build();
        updateViewHook.setViewToMerge(new View.Builder()
                .entity("entity2")
                .edge("edge2")
                .build());
        updateViewHook.preExecute(opChain, new Context(new User.Builder().opAuth("opA").build()));

        // When
        final Object op = opChain.getOperations().get(0);

        // Then
        assertTrue(op instanceof OperationView);
        OperationView opView = (OperationView) op;
        assertEquals(Sets.newHashSet("entity1"), opView.getView().getEntityGroups());
        assertEquals(Sets.newHashSet("edge1"), opView.getView().getEdgeGroups());
    }

    @Test
    public void shouldNotAddExtraGroupsToUsersViewInGetAdjacentIds() {
        // Given
        opChain = new OperationChain.Builder()
                .first(new GetAdjacentIds.Builder()
                        .view(new View.Builder()
                                .edge("edge1")
                                .build())
                        .build())
                .build();
        updateViewHook.setViewToMerge(new View.Builder()
                .entity("entity2")
                .edge("edge2")
                .build());
        updateViewHook.preExecute(opChain, new Context(new User.Builder().opAuth("opA").build()));

        // When
        Object op = opChain.getOperations().get(0);

        // Then
        assertTrue(op instanceof OperationView);
        OperationView opView = (OperationView) op;
        assertEquals(Sets.newHashSet(), opView.getView().getEntityGroups());
        assertEquals(Sets.newHashSet("edge1"), opView.getView().getEdgeGroups());
    }

    @Test
    public void shouldAddExtraGroupsToUsersView() {
        // Given
        opChain = new OperationChain.Builder()
                .first(new GetAllElements.Builder()
                        .view(new View.Builder()
                                .entity("entity1")
                                .edge("edge1")
                                .build())
                        .build())
                .build();
        updateViewHook.setViewToMerge(new View.Builder()
                .entity("entity2")
                .edge("edge2")
                .build());
        updateViewHook.setAddExtraGroups(true);
        updateViewHook.preExecute(opChain, new Context(new User.Builder().opAuth("opA").build()));

        // When
        final Object op = opChain.getOperations().get(0);

        // Then
        assertTrue(op instanceof OperationView);
        OperationView opView = (OperationView) op;
        assertEquals(Sets.newHashSet("entity1", "entity2"), opView.getView().getEntityGroups());
        assertEquals(Sets.newHashSet("edge1", "edge2"), opView.getView().getEdgeGroups());
    }

    @Test
    public void shouldNotAddExtraGroupsToEmptyUsersView() {
        // Given
        opChain = new OperationChain.Builder()
                .first(new GetAllElements.Builder()
                        .view(new View())
                        .build())
                .build();
        updateViewHook.setViewToMerge(new View.Builder()
                .entity("entity2")
                .edge("edge2")
                .build());
        updateViewHook.preExecute(opChain, new Context(new User.Builder().opAuth("opA").build()));

        // When
        final Object op = opChain.getOperations().get(0);

        // Then
        assertTrue(op instanceof OperationView);
        OperationView opView = (OperationView) op;
        assertEquals(Sets.newHashSet(), opView.getView().getEntityGroups());
        assertEquals(Sets.newHashSet(), opView.getView().getEdgeGroups());
    }

    @Test
    public void shouldNotMergeWithWrongUser() {
        // Given
        updateViewHook.setViewToMerge(viewToMerge);
        updateViewHook.setWithOpAuth(Sets.newHashSet("opA"));

        opChain = new OperationChain<>(new GetAllElements());
        updateViewHook.preExecute(opChain, new Context(new User()));

        // When
        final Object op = opChain.getOperations().get(0);

        // Then
        assertTrue(op instanceof OperationView);
        OperationView opView = (OperationView) op;
        assertNull(opView.getView());
    }

    @Test
    public void shouldMergeWithUser() {
        // Given
        updateViewHook.setViewToMerge(viewToMerge);
        updateViewHook.setWithOpAuth(Sets.newHashSet("opA"));

        opChain = new OperationChain(new GetAllElements());
        updateViewHook.preExecute(opChain, new Context(new User.Builder().opAuth("opA").build()));

        // When
        final Object op = opChain.getOperations().get(0);

        // Then
        assertTrue(op instanceof OperationView);
        OperationView opView = (OperationView) op;
        assertTrue(opView.getView().getGroups().contains("testGroup"));
    }

    @Test
    public void shouldMergeAndApplyWhiteList() {
        // Given
        updateViewHook.setViewToMerge(viewToMerge);
        updateViewHook.setWithOpAuth(Sets.newHashSet("opA"));
        updateViewHook.setWhiteListElementGroups(Sets.newHashSet("white2", "white1", "testGroup"));

        opChain = new OperationChain.Builder()
                .first(new GetAllElements.Builder()
                        .view(new View.Builder()
                                .entity("wrong1")
                                .entity("white1")
                                .entity("white2")
                                .edge("testGroup", new ViewElementDefinition.Builder()
                                        .preAggregationFilter(new ElementFilter.Builder()
                                                .select("prop1")
                                                .execute(new Exists())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();

        // When
        updateViewHook.preExecute(opChain, new Context(new User.Builder().opAuth("opA").build()));
        final GetAllElements op = (GetAllElements) opChain.getOperations().get(0);

        // Then
        final byte[] expected = new View.Builder()
                .entity("white1", new ViewElementDefinition.Builder()
                        .preAggregationFilter(new ElementFilter.Builder()
                                .select("prop1")
                                .execute(new IsIn("value1", "value2"))
                                .build())
                        .build())
                .entity("white2")
                .edge("testGroup", new ViewElementDefinition.Builder()
                        .preAggregationFilter(new ElementFilter.Builder()
                                .select("prop1")
                                .execute(new Exists())
                                .select("count")
                                .execute(new IsMoreThan(10))
                                .build())
                        .build())
                .build().toJson(true);
        JsonAssert.assertEquals(expected, op.getView().toJson(true));
        assertTrue(op.getView().getGroups().contains("testGroup"));
    }

    @Test
    public void shouldApplyWhiteAndBlackLists() {
        // Given
        opChain = new OperationChain.Builder()
                .first(new GetAllElements.Builder()
                        .view(new View.Builder()
                                .entity("wrong1")
                                .entity("white1")
                                .entity("white2")
                                .build())
                        .build())
                .build();
        updateViewHook.setWhiteListElementGroups(Sets.newHashSet("white2", "white1"));
        updateViewHook.setBlackListElementGroups(Sets.newHashSet("white1"));
        updateViewHook.preExecute(opChain, new Context(new User.Builder().opAuth("opA").build()));

        // When
        final Object op = opChain.getOperations().get(0);

        // Then
        assertTrue(op instanceof OperationView);
        OperationView opView = (OperationView) op;
        assertTrue(opView.getView().getEntities().containsKey("white2"));
        assertEquals(1, opView.getView().getEntities().keySet().size());
    }

    @Test
    public void shouldApplyWhiteLists() {
        // Given
        opChain = new OperationChain.Builder()
                .first(new GetAllElements.Builder()
                        .view(new View.Builder()
                                .entity("wrong1")
                                .entity("white1")
                                .entity("white2")
                                .build())
                        .build())
                .build();
        updateViewHook.setWhiteListElementGroups(Sets.newHashSet("white2", "white1"));
        updateViewHook.preExecute(opChain, new Context(new User.Builder().opAuth("opA").build()));

        // When
        final Object op = opChain.getOperations().get(0);

        // Then
        assertTrue(op instanceof OperationView);
        final OperationView opView = (OperationView) op;
        assertTrue(opView.getView().getEntities().containsKey("white2"));
        assertTrue(opView.getView().getEntities().containsKey("white1"));
        assertEquals(2, opView.getView().getEntities().keySet().size());
    }


    @Test
    public void shouldApplyBlackLists() {
        // Given
        opChain = new OperationChain.Builder()
                .first(new GetAllElements.Builder()
                        .view(new View.Builder()
                                .entity("white1")
                                .entity("white2")
                                .build())
                        .build())
                .build();
        updateViewHook.setBlackListElementGroups(Sets.newHashSet("white1"));
        updateViewHook.preExecute(opChain, new Context(new User.Builder().opAuth("opA").build()));

        // When
        final Object op = opChain.getOperations().get(0);

        // Then
        assertTrue(op instanceof OperationView);
        OperationView opView = (OperationView) op;
        assertTrue(opView.getView().getEntities().containsKey("white2"));
        assertEquals(1, opView.getView().getEntities().keySet().size());
    }

    @Test
    public void shouldDoNothingWithNullMerge() {
        // Given
        final GetAllElements operationView = new GetAllElements();
        operationView.setView(new View());

        // When
        final View view = updateViewHook.mergeView(operationView, null).build();

        // Then
        assertTrue(view.getGroups().isEmpty());
    }

    @Test
    public void shouldMerge() {
        // Given
        final GetAllElements operationView = new GetAllElements();
        operationView.setView(new View());

        // When
        final View view = updateViewHook.mergeView(operationView, viewToMerge).build();
        final Set<String> groups = view.getGroups();

        // Then
        assertFalse(groups.isEmpty());
        assertTrue(groups.contains("testGroup"));
    }

    @Test
    public void shouldDoNothingReturnResult() {
        // Given
        final String testString = "testString";

        // When / Then
        assertEquals(testString, updateViewHook.postExecute(testString, null, null));
        assertEquals(testString, updateViewHook.onFailure(testString, null, null, null));
    }

    //***** ApplyToUserTests *****

    @Test
    public void shouldPassWithOnlyOps() {
        // Given
        userOpAuths.add("oA");
        opAuths.add("oA");

        // When
        userBuilder.opAuths(opAuths);
        updateViewHook.setWithOpAuth(opAuths);

        // Then
        assertTrue(updateViewHook.applyToUser(userBuilder.build()));
    }

    @Test
    public void shouldPassWithOnlyData() {
        // Given
        updateViewHook.setWithOpAuth(null);

        // When / Then
        final String message = "updateViewHook.getWithOpAuth() needs to be empty for this test";
        assertTrue(updateViewHook.getWithOpAuth() == null || updateViewHook.getWithOpAuth().isEmpty(), message);

        // When
        userDataAuths.add("dA");
        dataAuths.add("dA");

        userBuilder.dataAuths(userDataAuths);
        updateViewHook.setWithDataAuth(dataAuths);

        // Then
        assertTrue(updateViewHook.applyToUser(userBuilder.build()));
    }

    @Test
    public void shouldPassWithBoth() {
        // Given
        userDataAuths.add("dA");
        dataAuths.add("dA");
        userOpAuths.add("oA");
        opAuths.add("oA");

        // When
        userBuilder.dataAuths(userDataAuths);
        userBuilder.opAuths(userOpAuths);
        updateViewHook.setWithDataAuth(dataAuths);
        updateViewHook.setWithOpAuth(opAuths);

        // Then
        assertTrue(updateViewHook.applyToUser(userBuilder.build()));
    }

    @Test
    public void shouldFailWithWrongOps() {
        // Given
        userOpAuths.add("oB");
        opAuths.add("oA");

        // When
        userBuilder.opAuths(userOpAuths);
        updateViewHook.setWithOpAuth(opAuths);

        // Then
        assertFalse(updateViewHook.applyToUser(userBuilder.build()));
    }

    @Test
    public void shouldFailWithWrongData() {
        // Given
        userDataAuths.add("dA");
        dataAuths.add("dB");

        // When
        userBuilder.dataAuths(userDataAuths);
        updateViewHook.setWithDataAuth(dataAuths);

        // Then
        assertFalse(updateViewHook.applyToUser(userBuilder.build()));
    }

    @Test
    public void shouldFailWithBothWrongOPsData() {
        // Given
        userDataAuths.add("dB");
        dataAuths.add("dA");
        userOpAuths.add("oB");
        opAuths.add("oA");

        // When
        userBuilder.dataAuths(userDataAuths);
        userBuilder.opAuths(userOpAuths);
        updateViewHook.setWithDataAuth(dataAuths);
        updateViewHook.setWithOpAuth(opAuths);

        // Then
        assertFalse(updateViewHook.applyToUser(userBuilder.build()));
    }

    @Test
    public void shouldFailWithOneWrongOPs() {
        // Given
        userDataAuths.add("dA");
        dataAuths.add("dA");
        userOpAuths.add("oB");
        opAuths.add("oA");

        // When
        userBuilder.dataAuths(userDataAuths);
        userBuilder.opAuths(userOpAuths);
        updateViewHook.setWithDataAuth(dataAuths);
        updateViewHook.setWithOpAuth(opAuths);

        // Then
        assertFalse(updateViewHook.applyToUser(userBuilder.build()));
    }

    @Test
    public void shouldFailWithOneWrongData() {
        // Given
        userDataAuths.add("dB");
        dataAuths.add("dA");
        userOpAuths.add("oA");
        opAuths.add("oA");

        // When
        userBuilder.dataAuths(userDataAuths);
        userBuilder.opAuths(userOpAuths);
        updateViewHook.setWithDataAuth(dataAuths);
        updateViewHook.setWithOpAuth(opAuths);

        // Then
        assertFalse(updateViewHook.applyToUser(userBuilder.build()));
    }

    //***** Serialise Tests *****

    @Test
    public void shouldSerialiseOpAuth() throws Exception {
        // Given
        final UpdateViewHook updateViewHook = new UpdateViewHook.Builder()
                .withOpAuth(Sets.newHashSet(TEST_WITH_VALUE))
                .withoutOpAuth(Sets.newHashSet(TEST_WITHOUT_VALUE))
                .build();
        final byte[] serialise = getBytes(updateViewHook);

        // When
        final UpdateViewHook deserialise = JSONSerialiser.deserialise(serialise, UpdateViewHook.class);

        // Then
        assertTrue(deserialise.getWithOpAuth().contains(TEST_WITH_VALUE));
        assertTrue(deserialise.getWithoutOpAuth().contains(TEST_WITHOUT_VALUE));
    }

    private byte[] getBytes(final UpdateViewHook updateViewHook) throws SerialisationException {
        byte[] serialise = JSONSerialiser.serialise(updateViewHook, true);
        String s = new String(serialise);
        assertTrue(s.contains(TEST_WITH_VALUE));
        assertTrue(s.contains(TEST_WITHOUT_VALUE));
        return serialise;
    }

    @Test
    public void shouldSerialiseDataAuths() throws Exception {
        // Given
        final UpdateViewHook updateViewHook = new UpdateViewHook.Builder()
                .withDataAuth(Sets.newHashSet(TEST_WITH_VALUE))
                .withoutDataAuth(Sets.newHashSet(TEST_WITHOUT_VALUE))
                .build();
        final byte[] serialise = getBytes(updateViewHook);

        // When
        final UpdateViewHook deserialise = JSONSerialiser.deserialise(serialise, UpdateViewHook.class);

        // Then
        assertTrue(deserialise.getWithDataAuth().contains(TEST_WITH_VALUE));
        assertTrue(deserialise.getWithoutDataAuth().contains(TEST_WITHOUT_VALUE));
    }

    @Test
    public void shouldSerialiseElementGroups() throws Exception {
        // Given
        final UpdateViewHook updateViewHook = new UpdateViewHook.Builder()
                .whiteListElementGroups(Sets.newHashSet(TEST_WITH_VALUE))
                .blackListElementGroups(Sets.newHashSet(TEST_WITHOUT_VALUE))
                .build();
        final byte[] serialise = getBytes(updateViewHook);

        // When
        final UpdateViewHook deserialise = JSONSerialiser.deserialise(serialise, UpdateViewHook.class);

        // Then
        assertTrue(deserialise.getWhiteListElementGroups().contains(TEST_WITH_VALUE));
        assertTrue(deserialise.getBlackListElementGroups().contains(TEST_WITHOUT_VALUE));
    }

    @Test
    public void shouldSerialiseViewToMerge() throws Exception {
        // Given
        final View viewToMerge = new View.Builder().entity(TEST_EDGE).build();
        final UpdateViewHook updateViewHook = new UpdateViewHook.Builder()
                .setViewToMerge(viewToMerge).build();
        final byte[] serialise = JSONSerialiser.serialise(updateViewHook, true);

        // When
        final String serialisedString = new String(serialise);
        assertTrue(serialisedString.contains(TEST_EDGE), serialisedString);

        // Then
        final UpdateViewHook deserialise = JSONSerialiser.deserialise(serialise, UpdateViewHook.class);
        assertEquals(deserialise.getViewToMerge(), viewToMerge);
    }

    public static final String TEST_KEY = "testKey";
    public static final String OTHER = "other";

    @Test
    public void shouldRemoveBlackList() {
        // Given
        final UpdateViewHook updateViewHook = new UpdateViewHook();
        updateViewHook.setBlackListElementGroups(Sets.newHashSet(TEST_KEY));

        // When / Then
        assertTrue(updateViewHook.removeElementGroups(getEntry()));
    }

    @Test
    public void shouldKeepWhiteList() {
        // Given
        final UpdateViewHook updateViewHook = new UpdateViewHook();
        updateViewHook.setWhiteListElementGroups(Sets.newHashSet(TEST_KEY));

        // When / Then
        assertFalse(updateViewHook.removeElementGroups(getEntry()));
    }

    @Test
    public void shouldRemoveInBothLists() {
        // Given
        final UpdateViewHook updateViewHook = new UpdateViewHook();
        updateViewHook.setBlackListElementGroups(Sets.newHashSet(TEST_KEY));
        updateViewHook.setWhiteListElementGroups(Sets.newHashSet(TEST_KEY));

        // When / Then
        assertTrue(updateViewHook.removeElementGroups(getEntry()));
    }

    @Test
    public void shouldKeepWhiteList2() {
        // Given
        final UpdateViewHook updateViewHook = new UpdateViewHook();
        updateViewHook.setWhiteListElementGroups(Sets.newHashSet(TEST_KEY));
        updateViewHook.setBlackListElementGroups(Sets.newHashSet(OTHER));

        // When / Then
        assertFalse(updateViewHook.removeElementGroups(getEntry()));
    }

    @Test
    public void shouldRemoveBlackList2() {
        // Given
        final UpdateViewHook updateViewHook = new UpdateViewHook();
        updateViewHook.setWhiteListElementGroups(Sets.newHashSet(OTHER));
        updateViewHook.setBlackListElementGroups(Sets.newHashSet(TEST_KEY));

        // When / Then
        assertTrue(updateViewHook.removeElementGroups(getEntry()));
    }


    private Entry<String, ViewElementDefinition> getEntry() {
        return new Entry<String, ViewElementDefinition>() {
            @Override
            public String getKey() {
                return TEST_KEY;
            }

            @Override
            public ViewElementDefinition getValue() {
                throw new UnsupportedOperationException("Not yet implemented.");
            }

            @Override
            public ViewElementDefinition setValue(final ViewElementDefinition value) {
                throw new UnsupportedOperationException("Not yet implemented.");
            }
        };
    }

    //***** VALIDATE AUTHS TESTS *****

    @Test
    public void shouldPassExcessAuth() {
        // Given
        userAuths.add(A);
        userAuths.add(B);
        validAuths.add(A);

        // When / Then
        assertTrue(updateViewHook.validateAuths(userAuths, validAuths, true));
    }

    @Test
    public void shouldPassSubsetAuth() {
        // Given
        userAuths.add(A);
        validAuths.add(A);
        validAuths.add(B);

        // When / Then
        assertTrue(updateViewHook.validateAuths(userAuths, validAuths, true));
    }

    @Test
    public void shouldFailMissingAuth() {
        // Given
        userAuths.add(B);
        validAuths.add(A);

        // When / Then
        assertFalse(updateViewHook.validateAuths(userAuths, validAuths, true));
    }

    @Test
    public void shouldFailEmptyUserAuths() {
        // Given
        validAuths.add(A);

        // When / Then
        assertFalse(updateViewHook.validateAuths(userAuths, validAuths, true));
    }

    @Test
    public void shouldFailNullUserAuths() {
        // Given
        validAuths.add(A);

        // When / Then
        assertFalse(updateViewHook.validateAuths(null, validAuths, true));
    }

    @Test
    public void shouldPassNullValid() {
        assertTrue(updateViewHook.validateAuths(userAuths, null, true));
    }

    @Override
    protected UpdateViewHook getTestObject() {
        return new UpdateViewHook();
    }
}
