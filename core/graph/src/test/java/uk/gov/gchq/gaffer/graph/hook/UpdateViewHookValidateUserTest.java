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

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.user.User.Builder;

import java.util.HashSet;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UpdateViewHookValidateUserTest {

    private UpdateViewHook updateViewHook;

    private HashSet<String> userOpAuths = Sets.newHashSet();
    private HashSet<String> userDataAuths = Sets.newHashSet();
    private HashSet<String> opAuths = Sets.newHashSet();
    private HashSet<String> dataAuths = Sets.newHashSet();
    private Builder userBuilder;

    @Before
    public void setUp() throws Exception {
        userOpAuths.clear();
        userDataAuths.clear();
        opAuths.clear();
        dataAuths.clear();

        updateViewHook = new UpdateViewHook();
        userBuilder = new Builder();
    }

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
}