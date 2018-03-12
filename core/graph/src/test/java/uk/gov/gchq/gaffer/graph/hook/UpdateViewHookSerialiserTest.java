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
import org.junit.Test;

import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View.Builder;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import static org.junit.Assert.assertTrue;

public class UpdateViewHookSerialiserTest {

    public static final String TEST_VALUE = "testValue";
    public static final String TEST_EDGE = "testEdge";

    @Test
    public void shouldSerialiseOpAuths() throws Exception {

        UpdateViewHook updateViewHook = new UpdateViewHook()
                .setOpAuths(Sets.newHashSet(TEST_VALUE));

        byte[] serialise = JSONSerialiser.serialise(updateViewHook, true);
        String s = new String(serialise);
        assertTrue(s.contains(TEST_VALUE));

        UpdateViewHook deserialise = JSONSerialiser.deserialise(serialise, UpdateViewHook.class);
        assertTrue(deserialise.getOpAuths().contains(TEST_VALUE));
    }

    @Test
    public void shouldSerialiseDataAuths() throws Exception {

        UpdateViewHook updateViewHook = new UpdateViewHook()
                .setDataAuths(Sets.newHashSet(TEST_VALUE));

        byte[] serialise = JSONSerialiser.serialise(updateViewHook, true);
        String s = new String(serialise);
        assertTrue(s.contains(TEST_VALUE));

        UpdateViewHook deserialise = JSONSerialiser.deserialise(serialise, UpdateViewHook.class);
        assertTrue(deserialise.getDataAuths().contains(TEST_VALUE));
    }

    @Test
    public void shouldSerialiseRestrictedGroups() throws Exception {

        UpdateViewHook updateViewHook = new UpdateViewHook()
                .setRestrictedGroups(Lists.newArrayList(TEST_VALUE));

        byte[] serialise = JSONSerialiser.serialise(updateViewHook, true);
        String s = new String(serialise);
        assertTrue(s.contains(TEST_VALUE));

        UpdateViewHook deserialise = JSONSerialiser.deserialise(serialise, UpdateViewHook.class);
        assertTrue(deserialise.getRestrictedGroups().contains(TEST_VALUE));
    }

    @Test
    public void shouldSerialiseViewToMerge() throws Exception {

        View viewToMerge = new Builder().entity(TEST_EDGE).build();
        UpdateViewHook updateViewHook = new UpdateViewHook()
                .setViewToMerge(viewToMerge);

        byte[] serialise = JSONSerialiser.serialise(updateViewHook, true);
        String s = new String(serialise);
        assertTrue(s.contains(TEST_EDGE));

        UpdateViewHook deserialise = JSONSerialiser.deserialise(serialise, UpdateViewHook.class);
        assertTrue(deserialise.getViewToMerge().equals(viewToMerge));
    }
}