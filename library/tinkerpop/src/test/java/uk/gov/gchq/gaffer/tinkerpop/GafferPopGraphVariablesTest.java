/*
 * Copyright 2023-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.tinkerpop;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.user.User;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class GafferPopGraphVariablesTest {
    GafferPopGraph graph = mock(GafferPopGraph.class);
    private final GafferPopGraphVariables variables = createVariables();

    @Test
    void shouldRemoveValueFromVariables() {
        // Given
        given(graph.variables()).willReturn(variables);

        // When
        graph.variables().remove(GafferPopGraphVariables.USER_ID);

        // Then
        assertThat(graph.variables().asMap()).hasSize(2);
    }

    @Test
    void shouldAllowSettingGafferPopVariables() {
        // Given
        given(graph.variables()).willReturn(variables);
        final String testUserId = "testUserId";
        final String[] testDataAuths = {"auth1", "auth2"};
        final User testUser = new User.Builder()
                .userId(testUserId)
                .dataAuths(testDataAuths)
                .build();
        final List<String> testOpOptions = Arrays.asList("graphId:graph1", "other:other");
        final GafferPopGraphVariables graphVariables = (GafferPopGraphVariables) graph.variables();

        // When
        graphVariables.set(GafferPopGraphVariables.USER, testUser);
        graphVariables.set(GafferPopGraphVariables.DATA_AUTHS, testDataAuths);
        graphVariables.set(GafferPopGraphVariables.OP_OPTIONS, testOpOptions);

        // Then
        assertThat(graphVariables.getUser().getUserId()).isEqualTo(testUserId);
        assertThat(graphVariables.getUser().getDataAuths()).containsExactlyInAnyOrder(testDataAuths);
        assertThat(graphVariables.getOperationOptions()).containsOnly(
            entry("graphId", "graph1"),
            entry("other", "other"));
    }

    @Test
    void shouldReturnStringOfTotalNumberOfGraphVariables() {
        // Given
        given(graph.variables()).willReturn(variables);
        final Integer varSize = graph.variables().asMap().size();
        // Then
        assertThat(graph.variables().toString())
            .contains("variables", "size", varSize.toString());
    }

    private GafferPopGraphVariables createVariables() {
        final ConcurrentHashMap<String, Object> variablesMap = new ConcurrentHashMap<>();
        variablesMap.put(GafferPopGraphVariables.OP_OPTIONS, new String[] {"key1:value1", "key2:value2" });
        variablesMap.put(GafferPopGraphVariables.USER_ID, "user");
        variablesMap.put(GafferPopGraphVariables.DATA_AUTHS, "dataauth1,dataauth2");
        return new GafferPopGraphVariables(variablesMap);
    }

}
