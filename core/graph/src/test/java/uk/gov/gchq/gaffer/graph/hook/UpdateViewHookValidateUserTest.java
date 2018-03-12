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

    public static final String C = "C";
    private UpdateViewHook updateViewHook;
    public static final String A = "A";
    public static final String B = "B";
    private final HashSet<String> validAuths = Sets.newHashSet();
    private HashSet<String> userAuths = Sets.newHashSet();
    private Builder builder;

    @Before
    public void setUp() throws Exception {
        userAuths.clear();
        validAuths.clear();
        updateViewHook = new UpdateViewHook();
        builder = new Builder();
    }

    @Test
    public void shouldPassWithOnlyOps() throws Exception {
        userAuths.add(A);
        validAuths.add(A);


        builder.opAuths(userAuths);
        updateViewHook.setOpAuths(validAuths);

        assertTrue(updateViewHook.validateUser(builder.build()));
    }

    @Test
    public void shouldPassWithOnlyData() throws Exception {
        userAuths.add(A);
        validAuths.add(A);

        builder.dataAuths(userAuths);
        updateViewHook.setDataAuths(validAuths);

        assertTrue(updateViewHook.validateUser(builder.build()));
    }

    @Test
    public void shouldPassWithBothOPsData() throws Exception {
        validAuths.add(A);
        HashSet<String> altValid = Sets.newHashSet(B);

        builder.opAuths(Sets.newHashSet(A)).dataAuths(Sets.newHashSet(B));
        updateViewHook.setOpAuths(validAuths);
        updateViewHook.setDataAuths(altValid);
        
        assertTrue(updateViewHook.validateUser(builder.build()));
    }



    @Test
    public void shouldFailWithWrongOnlyOps() throws Exception {
        userAuths.add(A);
        validAuths.add(B);

        builder.opAuths(userAuths);
        updateViewHook.setOpAuths(validAuths);

        assertFalse(updateViewHook.validateUser(builder.build()));
    }

    @Test
    public void shouldFailWithWrongOnlyData() throws Exception {
        userAuths.add(A);
        validAuths.add(B);

        builder.dataAuths(userAuths);
        updateViewHook.setDataAuths(validAuths);

        assertFalse(updateViewHook.validateUser(builder.build()));
    }

    @Test
    public void shouldFailWithBothWrongOPsData() throws Exception {
        userAuths.add(A);
        HashSet<String> alt = Sets.newHashSet(B);
        validAuths.add(C);

        builder.opAuths(userAuths).dataAuths(userAuths);
        updateViewHook.setOpAuths(validAuths);
        updateViewHook.setDataAuths(alt);

        assertFalse(updateViewHook.validateUser(builder.build()));
    }

    @Test
    public void shouldFailWithOneWrongOPs() throws Exception {
        userAuths.add(A);
        validAuths.add(A);
        HashSet<String> alt = Sets.newHashSet(B);

        builder.opAuths(userAuths).dataAuths(userAuths);
        updateViewHook.setOpAuths(validAuths);
        updateViewHook.setDataAuths(alt);

        assertFalse(updateViewHook.validateUser(builder.build()));
    }

    @Test
    public void shouldFailWithOneWrongData() throws Exception {
        userAuths.add(A);
        validAuths.add(B);
        HashSet<String> alt = Sets.newHashSet(A);

        builder.opAuths(userAuths).dataAuths(userAuths);
        updateViewHook.setOpAuths(validAuths);
        updateViewHook.setDataAuths(alt);

        assertFalse(updateViewHook.validateUser(builder.build()));
    }
}