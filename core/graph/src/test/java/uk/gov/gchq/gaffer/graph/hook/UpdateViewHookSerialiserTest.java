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
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import static org.junit.Assert.assertTrue;

public class UpdateViewHookSerialiserTest {

    public static final String TEST_WITH_VALUE = "withTestValue";
    public static final String TEST_WITHOUT_VALUE = "withoutTestValue";
    public static final String TEST_EDGE = "testEdge";

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
                .whiteListElementGroups(Lists.newArrayList(TEST_WITH_VALUE))
                .blackListElementGroups(Lists.newArrayList(TEST_WITHOUT_VALUE))
                .build();

        byte[] serialise = getBytes(updateViewHook);

        UpdateViewHook deserialise = JSONSerialiser.deserialise(serialise, UpdateViewHook.class);
        assertTrue(deserialise.getWhiteListElementGroups().contains(TEST_WITH_VALUE));
        assertTrue(deserialise.getBlackListElementGroups().contains(TEST_WITHOUT_VALUE));
    }

    @Test
    public void shouldSerialiseViewToMerge() throws Exception {

        View viewToMerge = new Builder().entity(TEST_EDGE).build();
        UpdateViewHook updateViewHook = new UpdateViewHook.Builder()
                .setViewToMerge(viewToMerge).build();

        byte[] serialise = JSONSerialiser.serialise(updateViewHook, true);
        String s = new String(serialise);
        assertTrue(s,s.contains(TEST_EDGE));

        UpdateViewHook deserialise = JSONSerialiser.deserialise(serialise, UpdateViewHook.class);
        assertTrue(deserialise.getViewToMerge().equals(viewToMerge));
    }
}