/*
 * Copyright 2018-2019 Crown Copyright
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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

    @Before
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
    public void shouldNotAddExtraGroupsToUsersView() throws Exception {
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

        Object op = opChain.getOperations().get(0);
        if (op instanceof OperationView) {
            OperationView opView = (OperationView) op;
            assertEquals(Sets.newHashSet("entity1"), opView.getView().getEntityGroups());
            assertEquals(Sets.newHashSet("edge1"), opView.getView().getEdgeGroups());
        } else {
            fail("unexpected operation found.");
        }
    }

    @Test
    public void shouldNotAddExtraGroupsToUsersViewInGetAdjacentIds() throws Exception {
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

        Object op = opChain.getOperations().get(0);
        if (op instanceof OperationView) {
            OperationView opView = (OperationView) op;
            assertEquals(Sets.newHashSet(), opView.getView().getEntityGroups());
            assertEquals(Sets.newHashSet("edge1"), opView.getView().getEdgeGroups());
        } else {
            fail("unexpected operation found.");
        }
    }

    @Test
    public void shouldAddExtraGroupsToUsersView() throws Exception {
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

        Object op = opChain.getOperations().get(0);
        if (op instanceof OperationView) {
            OperationView opView = (OperationView) op;
            assertEquals(Sets.newHashSet("entity1", "entity2"), opView.getView().getEntityGroups());
            assertEquals(Sets.newHashSet("edge1", "edge2"), opView.getView().getEdgeGroups());
        } else {
            fail("unexpected operation found.");
        }
    }

    @Test
    public void shouldNotAddExtraGroupsToEmptyUsersView() throws Exception {
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

        Object op = opChain.getOperations().get(0);
        if (op instanceof OperationView) {
            OperationView opView = (OperationView) op;
            assertEquals(Sets.newHashSet(), opView.getView().getEntityGroups());
            assertEquals(Sets.newHashSet(), opView.getView().getEdgeGroups());
        } else {
            fail("unexpected operation found.");
        }
    }

    @Test
    public void shouldNotMergeWithWrongUser() throws Exception {
        updateViewHook.setViewToMerge(viewToMerge);
        updateViewHook.setWithOpAuth(Sets.newHashSet("opA"));

        opChain = new OperationChain<>(new GetAllElements());
        updateViewHook.preExecute(opChain, new Context(new User()));
        Object op = opChain.getOperations().get(0);
        if (op instanceof OperationView) {
            OperationView opView = (OperationView) op;
            assertNull(opView.getView());
        } else {
            fail("unexpected operation found.");
        }
    }

    @Test
    public void shouldMergeWithUser() throws Exception {
        updateViewHook.setViewToMerge(viewToMerge);
        updateViewHook.setWithOpAuth(Sets.newHashSet("opA"));

        opChain = new OperationChain(new GetAllElements());
        updateViewHook.preExecute(opChain, new Context(new User.Builder().opAuth("opA").build()));
        Object op = opChain.getOperations().get(0);
        if (op instanceof OperationView) {
            OperationView opView = (OperationView) op;
            assertTrue(opView.getView().getGroups().contains("testGroup"));
        } else {
            fail("unexpected operation found.");
        }
    }

    @Test
    public void shouldMergeAndApplyWhiteList() throws Exception {
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

        updateViewHook.preExecute(opChain, new Context(new User.Builder().opAuth("opA").build()));
        GetAllElements op = (GetAllElements) opChain.getOperations().get(0);
        JsonAssert.assertEquals(
                new View.Builder()
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
                        .build().toJson(true),
                op.getView().toJson(true)
        );
        assertTrue(op.getView().getGroups().contains("testGroup"));
    }

    @Test
    public void shouldApplyWhiteAndBlackLists() throws Exception {
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

        Object op = opChain.getOperations().get(0);
        if (op instanceof OperationView) {
            OperationView opView = (OperationView) op;
            assertTrue(opView.getView().getEntities().keySet().contains("white2"));
            assertEquals(1, opView.getView().getEntities().keySet().size());
        } else {
            fail("unexpected operation found.");
        }
    }

    @Test
    public void shouldApplyWhiteLists() throws Exception {
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

        Object op = opChain.getOperations().get(0);
        if (op instanceof OperationView) {
            OperationView opView = (OperationView) op;
            assertTrue(opView.getView().getEntities().keySet().contains("white2"));
            assertTrue(opView.getView().getEntities().keySet().contains("white1"));
            assertEquals(2, opView.getView().getEntities().keySet().size());
        } else {
            fail("unexpected operation found.");
        }
    }


    @Test
    public void shouldApplyBlackLists() throws Exception {
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

        Object op = opChain.getOperations().get(0);
        if (op instanceof OperationView) {
            OperationView opView = (OperationView) op;
            assertTrue(opView.getView().getEntities().keySet().contains("white2"));
            assertEquals(1, opView.getView().getEntities().keySet().size());
        } else {
            fail("unexpected operation found.");
        }
    }

    @Test
    public void shouldDoNothingWithNullMerge() throws Exception {
        GetAllElements operationView = new GetAllElements();
        operationView.setView(new View());
        View view = updateViewHook.mergeView(operationView, null).build();
        assertTrue(view.getGroups().isEmpty());
    }

    @Test
    public void shouldMerge() throws Exception {
        GetAllElements operationView = new GetAllElements();
        operationView.setView(new View());
        View view = updateViewHook.mergeView(operationView, viewToMerge).build();
        Set<String> groups = view.getGroups();
        assertFalse(groups.isEmpty());
        assertTrue(groups.contains("testGroup"));
    }

    @Test
    public void shouldDoNothingReturnResult() throws Exception {
        final String testString = "testString";
        assertEquals(testString, updateViewHook.postExecute(testString, null, null));
        assertEquals(testString, updateViewHook.onFailure(testString, null, null, null));

    }

    //***** ApplyToUserTests *****

    @Test
    public void shouldPassWithOnlyOps() throws Exception {
        userOpAuths.add("oA");
        opAuths.add("oA");

        userBuilder.opAuths(opAuths);
        updateViewHook.setWithOpAuth(opAuths);

        assertTrue(updateViewHook.applyToUser(userBuilder.build()));
    }

    @Test
    public void shouldPassWithOnlyData() throws Exception {
        updateViewHook.setWithOpAuth(null);

        assertTrue("updateViewHook.getWithOpAuth() needs to be empty for this test",
                updateViewHook.getWithOpAuth() == null || updateViewHook.getWithOpAuth().isEmpty());
        userDataAuths.add("dA");
        dataAuths.add("dA");

        userBuilder.dataAuths(userDataAuths);
        updateViewHook.setWithDataAuth(dataAuths);

        assertTrue(updateViewHook.applyToUser(userBuilder.build()));
    }

    @Test
    public void shouldPassWithBoth() throws Exception {
        userDataAuths.add("dA");
        dataAuths.add("dA");
        userOpAuths.add("oA");
        opAuths.add("oA");

        userBuilder.dataAuths(userDataAuths);
        userBuilder.opAuths(userOpAuths);
        updateViewHook.setWithDataAuth(dataAuths);
        updateViewHook.setWithOpAuth(opAuths);

        assertTrue(updateViewHook.applyToUser(userBuilder.build()));
    }


    @Test
    public void shouldFailWithWrongOps() throws Exception {
        userOpAuths.add("oB");
        opAuths.add("oA");

        userBuilder.opAuths(userOpAuths);
        updateViewHook.setWithOpAuth(opAuths);

        assertFalse(updateViewHook.applyToUser(userBuilder.build()));
    }

    @Test
    public void shouldFailWithWrongData() throws Exception {
        userDataAuths.add("dA");
        dataAuths.add("dB");

        userBuilder.dataAuths(userDataAuths);
        updateViewHook.setWithDataAuth(dataAuths);

        assertFalse(updateViewHook.applyToUser(userBuilder.build()));
    }

    @Test
    public void shouldFailWithBothWrongOPsData() throws Exception {
        userDataAuths.add("dB");
        dataAuths.add("dA");
        userOpAuths.add("oB");
        opAuths.add("oA");

        userBuilder.dataAuths(userDataAuths);
        userBuilder.opAuths(userOpAuths);
        updateViewHook.setWithDataAuth(dataAuths);
        updateViewHook.setWithOpAuth(opAuths);

        assertFalse(updateViewHook.applyToUser(userBuilder.build()));
    }

    @Test
    public void shouldFailWithOneWrongOPs() throws Exception {
        userDataAuths.add("dA");
        dataAuths.add("dA");
        userOpAuths.add("oB");
        opAuths.add("oA");

        userBuilder.dataAuths(userDataAuths);
        userBuilder.opAuths(userOpAuths);
        updateViewHook.setWithDataAuth(dataAuths);
        updateViewHook.setWithOpAuth(opAuths);

        assertFalse(updateViewHook.applyToUser(userBuilder.build()));
    }

    @Test
    public void shouldFailWithOneWrongData() throws Exception {
        userDataAuths.add("dB");
        dataAuths.add("dA");
        userOpAuths.add("oA");
        opAuths.add("oA");

        userBuilder.dataAuths(userDataAuths);
        userBuilder.opAuths(userOpAuths);
        updateViewHook.setWithDataAuth(dataAuths);
        updateViewHook.setWithOpAuth(opAuths);

        assertFalse(updateViewHook.applyToUser(userBuilder.build()));
    }

    //***** Serialise Tests *****

    @Test
    public void shouldSerialiseOpAuth() throws Exception {

        UpdateViewHook updateViewHook = new UpdateViewHook.Builder()
                .withOpAuth(Sets.newHashSet(TEST_WITH_VALUE))
                .withoutOpAuth(Sets.newHashSet(TEST_WITHOUT_VALUE))
                .build();

        byte[] serialise = getBytes(updateViewHook);

        UpdateViewHook deserialise = JSONSerialiser.deserialise(serialise, UpdateViewHook.class);
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

        UpdateViewHook updateViewHook = new UpdateViewHook.Builder()
                .withDataAuth(Sets.newHashSet(TEST_WITH_VALUE))
                .withoutDataAuth(Sets.newHashSet(TEST_WITHOUT_VALUE))
                .build();

        byte[] serialise = getBytes(updateViewHook);

        UpdateViewHook deserialise = JSONSerialiser.deserialise(serialise, UpdateViewHook.class);
        assertTrue(deserialise.getWithDataAuth().contains(TEST_WITH_VALUE));
        assertTrue(deserialise.getWithoutDataAuth().contains(TEST_WITHOUT_VALUE));
    }

    @Test
    public void shouldSerialiseElementGroups() throws Exception {

        UpdateViewHook updateViewHook = new UpdateViewHook.Builder()
                .whiteListElementGroups(Sets.newHashSet(TEST_WITH_VALUE))
                .blackListElementGroups(Sets.newHashSet(TEST_WITHOUT_VALUE))
                .build();

        byte[] serialise = getBytes(updateViewHook);

        UpdateViewHook deserialise = JSONSerialiser.deserialise(serialise, UpdateViewHook.class);
        assertTrue(deserialise.getWhiteListElementGroups().contains(TEST_WITH_VALUE));
        assertTrue(deserialise.getBlackListElementGroups().contains(TEST_WITHOUT_VALUE));
    }

    @Test
    public void shouldSerialiseViewToMerge() throws Exception {

        View viewToMerge = new View.Builder().entity(TEST_EDGE).build();
        UpdateViewHook updateViewHook = new UpdateViewHook.Builder()
                .setViewToMerge(viewToMerge).build();

        byte[] serialise = JSONSerialiser.serialise(updateViewHook, true);
        String s = new String(serialise);
        assertTrue(s, s.contains(TEST_EDGE));

        UpdateViewHook deserialise = JSONSerialiser.deserialise(serialise, UpdateViewHook.class);
        assertTrue(deserialise.getViewToMerge().equals(viewToMerge));
    }

    public static final String TEST_KEY = "testKey";
    public static final String OTHER = "other";

    @Test
    public void shouldRemoveBlackList() throws Exception {
        UpdateViewHook updateViewHook = new UpdateViewHook();
        updateViewHook.setBlackListElementGroups(Sets.newHashSet(TEST_KEY));

        Assert.assertTrue(updateViewHook.removeElementGroups(getEntry()));
    }

    @Test
    public void shouldKeepWhiteList() throws Exception {
        UpdateViewHook updateViewHook = new UpdateViewHook();
        updateViewHook.setWhiteListElementGroups(Sets.newHashSet(TEST_KEY));

        Assert.assertFalse(updateViewHook.removeElementGroups(getEntry()));
    }


    @Test
    public void shouldRemoveInBothLists() throws Exception {
        UpdateViewHook updateViewHook = new UpdateViewHook();
        updateViewHook.setBlackListElementGroups(Sets.newHashSet(TEST_KEY));
        updateViewHook.setWhiteListElementGroups(Sets.newHashSet(TEST_KEY));

        Assert.assertTrue(updateViewHook.removeElementGroups(getEntry()));
    }

    @Test
    public void shouldKeepWhiteList2() throws Exception {
        UpdateViewHook updateViewHook = new UpdateViewHook();
        updateViewHook.setWhiteListElementGroups(Sets.newHashSet(TEST_KEY));
        updateViewHook.setBlackListElementGroups(Sets.newHashSet(OTHER));

        Assert.assertFalse(updateViewHook.removeElementGroups(getEntry()));
    }

    @Test
    public void shouldRemoveBlackList2() throws Exception {
        UpdateViewHook updateViewHook = new UpdateViewHook();
        updateViewHook.setWhiteListElementGroups(Sets.newHashSet(OTHER));
        updateViewHook.setBlackListElementGroups(Sets.newHashSet(TEST_KEY));

        Assert.assertTrue(updateViewHook.removeElementGroups(getEntry()));
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
    public void shouldPassExcessAuth() throws Exception {
        userAuths.add(A);
        userAuths.add(B);
        validAuths.add(A);

        assertTrue(updateViewHook.validateAuths(userAuths, validAuths, true));
    }

    @Test
    public void shouldPassSubsetAuth() throws Exception {
        userAuths.add(A);
        validAuths.add(A);
        validAuths.add(B);

        assertTrue(updateViewHook.validateAuths(userAuths, validAuths, true));
    }

    @Test
    public void shouldFailMissingAuth() throws Exception {
        userAuths.add(B);
        validAuths.add(A);

        assertFalse(updateViewHook.validateAuths(userAuths, validAuths, true));
    }

    @Test
    public void shouldFailEmptyUserAuths() throws Exception {
        validAuths.add(A);

        assertFalse(updateViewHook.validateAuths(userAuths, validAuths, true));
    }

    @Test
    public void shouldFailNullUserAuths() throws Exception {
        validAuths.add(A);

        assertFalse(updateViewHook.validateAuths(null, validAuths, true));
    }

    @Test
    public void shouldPassNullValid() throws Exception {

        assertTrue(updateViewHook.validateAuths(userAuths, null, true));
    }

    @Override
    protected UpdateViewHook getTestObject() {
        return new UpdateViewHook();
    }
}
