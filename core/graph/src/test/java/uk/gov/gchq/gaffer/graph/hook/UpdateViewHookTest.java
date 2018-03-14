/*
 * Copyright 2018 Crown Copyright
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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.graph.OperationView;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.user.User;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class UpdateViewHookTest {

    private User user;
    private Context context;
    private OperationChain opChain;
    private UpdateViewHook updateViewHook;
    private View viewToMerge;

    @Before
    public void setUp() throws Exception {
        updateViewHook = new UpdateViewHook();
        viewToMerge = new View.Builder().edge("testGroup").build();
        updateViewHook.setViewToMerge(viewToMerge);
        updateViewHook.setWithOpAuth(Sets.newHashSet("opA"));
    }

    @Test
    public void shouldNotMergeWithWrongUser() throws Exception {
        opChain = new OperationChain(new GetAllElements());
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
    public void shouldApplyWhiteAndBlackLists() throws Exception {
        opChain = new OperationChain(new GetAllElements.Builder().view(new View.Builder().entity("wrong1").entity("white1").entity("white2").build()).build());
        updateViewHook.setWhiteListElementGroups(Lists.newArrayList("white2", "white1"));
        updateViewHook.setBlackListElementGroups(Lists.newArrayList("white1"));
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
        opChain = new OperationChain(new GetAllElements.Builder().view(new View.Builder().entity("wrong1").entity("white1").entity("white2").build()).build());
        updateViewHook.setWhiteListElementGroups(Lists.newArrayList("white2", "white1"));
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
        opChain = new OperationChain(new GetAllElements.Builder().view(new View.Builder().entity("white1").entity("white2").build()).build());
        updateViewHook.setBlackListElementGroups(Lists.newArrayList("white1"));
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
}