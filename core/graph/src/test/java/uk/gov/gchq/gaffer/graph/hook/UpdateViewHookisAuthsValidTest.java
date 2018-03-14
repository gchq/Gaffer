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

import java.util.HashSet;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UpdateViewHookIsAuthsValidTest {


    public static final String A = "A";
    public static final String B = "B";
    private final HashSet<String> validAuths = Sets.newHashSet();
    private HashSet<String> userAuths = Sets.newHashSet();
    private UpdateViewHook updateViewHook;

    @Before
    public void setUp() throws Exception {
        userAuths.clear();
        validAuths.clear();
        updateViewHook = new UpdateViewHook();
    }

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

}