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
import org.junit.Assert;
import org.junit.Test;

import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;

import java.util.Map.Entry;

public class UpdateViewHookRemoveElementGroupsTest {

    public static final String TEST_KEY = "testKey";
    public static final String OTHER = "other";

    @Test
    public void shouldRemoveBlackList() throws Exception {
        UpdateViewHook updateViewHook = new UpdateViewHook();
        updateViewHook.setBlackListElementGroups(Lists.newArrayList(TEST_KEY));

        Assert.assertTrue(updateViewHook.removeElementGroups(getEntry()));
    }

    @Test
    public void shouldKeepWhiteList() throws Exception {
        UpdateViewHook updateViewHook = new UpdateViewHook();
        updateViewHook.setWhiteListElementGroups(Lists.newArrayList(TEST_KEY));

        Assert.assertFalse(updateViewHook.removeElementGroups(getEntry()));
    }


    @Test
    public void shouldRemoveInBothLists() throws Exception {
        UpdateViewHook updateViewHook = new UpdateViewHook();
        updateViewHook.setBlackListElementGroups(Lists.newArrayList(TEST_KEY));
        updateViewHook.setWhiteListElementGroups(Lists.newArrayList(TEST_KEY));

        Assert.assertTrue(updateViewHook.removeElementGroups(getEntry()));
    }

    @Test
    public void shouldKeepWhiteList2() throws Exception {
        UpdateViewHook updateViewHook = new UpdateViewHook();
        updateViewHook.setWhiteListElementGroups(Lists.newArrayList(TEST_KEY));
        updateViewHook.setBlackListElementGroups(Lists.newArrayList(OTHER));

        Assert.assertFalse(updateViewHook.removeElementGroups(getEntry()));
    }

    @Test
    public void shouldRemoveBlackList2() throws Exception {
        UpdateViewHook updateViewHook = new UpdateViewHook();
        updateViewHook.setWhiteListElementGroups(Lists.newArrayList(OTHER));
        updateViewHook.setBlackListElementGroups(Lists.newArrayList(TEST_KEY));

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
}