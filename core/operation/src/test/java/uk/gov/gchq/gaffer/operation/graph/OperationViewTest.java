/*
 * Copyright 2017-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.graph;

import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.elementdefinition.view.NamedView;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertTrue;

public class OperationViewTest {
    private final View testView1 = new View.Builder()
            .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                    .excludeProperties("testProp")
                    .build())
            .build();
    private final View testView2 = new View.Builder()
            .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                    .allProperties()
                    .groupBy("group")
                    .build())
            .build();
    private final NamedView testNamedView1 = new NamedView.Builder()
            .name("testNamedView")
            .build();
    private final View mergedTestViews = new View.Builder().merge(testView1).merge(testView2).build();

    private OperationViewImpl operationView;

    @Before
    public void setUp() {
        operationView = new OperationViewImpl();
    }

    @Test
    public void shouldMergeTwoViewsWhenSettingBothAtOnce() {
        // When
        operationView.setViews(Arrays.asList(testView1, testView2));

        // Then
        JsonAssert.assertEquals(mergedTestViews.toCompactJson(), operationView.getView().toCompactJson());
    }

    @Test
    public void shouldMergeTwoViewsWhenOneIsNamedView() {
        // When
        operationView.setViews(Arrays.asList(testView1, testNamedView1));

        // Then - no exceptions
    }

    @Test
    public void shouldMergeTwoViewsWhenOneAlreadySet() {
        // When
        operationView.setView(testView1);
        operationView.setViews(Collections.singletonList(testView2));

        // Then
        JsonAssert.assertEquals(mergedTestViews.toCompactJson(), operationView.getView().toCompactJson());
    }

    @Test
    public void shouldMergeEmptyViewCorrectly() {
        // When
        operationView.setViews(Arrays.asList(testView1, new View()));

        // Then
        JsonAssert.assertEquals(testView1.toCompactJson(), operationView.getView().toCompactJson());
    }

    @Test
    public void shouldCorrectlyMergeIdenticalViewsWhenSettingBothAtOnce() {
        // When
        operationView.setViews(Arrays.asList(testView1, testView1));

        // Then
        JsonAssert.assertEquals(testView1.toCompactJson(), operationView.getView().toCompactJson());
    }

    @Test
    public void shouldCorrectlyMergeIdenticalViewsWhenOneAlreadySet() {
        // When
        operationView.setView(testView1);
        operationView.setViews(Collections.singletonList(testView1));

        // Then
        JsonAssert.assertEquals(testView1.toCompactJson(), operationView.getView().toCompactJson());
    }

    @Test
    public void shouldThrowExceptionWhenSettingViewsToNull() {
        try {
            operationView.setViews(null);
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Supplied View list cannot be null"));
        }
    }
}
