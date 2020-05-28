/*
 * Copyright 2017-2020 Crown Copyright
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.elementdefinition.view.NamedView;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static uk.gov.gchq.gaffer.commonutil.JsonAssert.assertEquals;

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

    @BeforeEach
    public void setUp() {
        operationView = new OperationViewImpl();
    }

    @Test
    public void shouldMergeTwoViewsWhenSettingBothAtOnce() {
        operationView.setViews(Arrays.asList(testView1, testView2));

        assertEquals(mergedTestViews.toCompactJson(), operationView.getView().toCompactJson());
    }

    @Test
    public void shouldMergeTwoViewsWhenOneIsNamedView() {
        assertDoesNotThrow(() -> operationView.setViews(Arrays.asList(testView1, testNamedView1)));
    }

    @Test
    public void shouldMergeTwoViewsWhenOneAlreadySet() {
        operationView.setView(testView1);
        operationView.setViews(Collections.singletonList(testView2));

        assertEquals(mergedTestViews.toCompactJson(), operationView.getView().toCompactJson());
    }

    @Test
    public void shouldMergeEmptyViewCorrectly() {
        operationView.setViews(Arrays.asList(testView1, new View()));

        assertEquals(testView1.toCompactJson(), operationView.getView().toCompactJson());
    }

    @Test
    public void shouldCorrectlyMergeIdenticalViewsWhenSettingBothAtOnce() {
        operationView.setViews(Arrays.asList(testView1, testView1));

        assertEquals(testView1.toCompactJson(), operationView.getView().toCompactJson());
    }

    @Test
    public void shouldCorrectlyMergeIdenticalViewsWhenOneAlreadySet() {
        operationView.setView(testView1);
        operationView.setViews(Collections.singletonList(testView1));

        assertEquals(testView1.toCompactJson(), operationView.getView().toCompactJson());
    }

    @Test
    public void doesNotThrowAnyExceptionsWhenSettingViewsToNull() {
        assertDoesNotThrow(() -> operationView.setViews(null));
    }
}
