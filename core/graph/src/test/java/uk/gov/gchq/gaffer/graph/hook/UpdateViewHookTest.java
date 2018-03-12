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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;

public class UpdateViewHookTest {


    public static final String A = "A";
    public static final String B = "B";
    private final HashSet<String> validAuths = Sets.newHashSet();
    private HashSet<String> userAuths = Sets.newHashSet();

    @Before
    public void setUp() throws Exception {
        userAuths.clear();
        validAuths.clear();
    }

    @Test
    public void shouldValidateWithExcess() throws Exception {
        userAuths.add(A);
        userAuths.add(B);
        validAuths.add(A);

        Assert.assertTrue(UpdateViewHook.isAuthsValid(userAuths, validAuths));
    }

    @Test
    public void shouldValidateSubset() throws Exception {
        userAuths.add(A);
        validAuths.add(A);
        validAuths.add(B);

        Assert.assertTrue(UpdateViewHook.isAuthsValid(userAuths, validAuths));
    }

    @Test
    public void shouldNotValidateMissing() throws Exception {
        userAuths.add(B);
        validAuths.add(A);

        Assert.assertFalse(UpdateViewHook.isAuthsValid(userAuths, validAuths));
    }

    @Test
    public void shouldNotValidateEmpty() throws Exception {
        validAuths.add(A);

        Assert.assertFalse(UpdateViewHook.isAuthsValid(userAuths, validAuths));
    }

    @Test
    public void shouldNotValidateNullUser() throws Exception {
        validAuths.add(A);

        Assert.assertFalse(UpdateViewHook.isAuthsValid(null, validAuths));
    }

    @Test
    public void shouldValidateNullValid() throws Exception {

        Assert.assertTrue(UpdateViewHook.isAuthsValid(userAuths, null));
    }

}