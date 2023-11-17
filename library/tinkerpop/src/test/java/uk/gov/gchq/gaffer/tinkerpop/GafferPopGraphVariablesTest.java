/*
 * Copyright 2023 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.StreamUtil;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class GafferPopGraphVariablesTest {
    GafferPopGraph graph = mock(GafferPopGraph.class);
    private final GafferPopGraphVariables variables = createVariables();

    @Test
    void shouldRemoveValueFromVariables() {
        given(graph.variables()).willReturn(variables);

        graph.variables().remove(GafferPopGraphVariables.SCHEMA);
        final Map<String, Object> graphVariables = graph.variables().asMap();

        assertThat(graphVariables).hasSize(2);
    }

    @Test
    void shouldThrowErrorWhenTryRemoveVariables() {
        given(graph.variables()).willReturn(variables);

        assertThatExceptionOfType(UnsupportedOperationException.class)
            .isThrownBy(() -> graph.variables().set("key1", "value1"));
    }

    @Test
    void shouldReturnStringOfGraphVariables() {
        given(graph.variables()).willReturn(variables);

        assertThat(graph.variables().toString())
            .hasToString(variables.toString());
    }

    private GafferPopGraphVariables createVariables() {
        final ConcurrentHashMap<String, Object> variablesMap = new ConcurrentHashMap<>();
        variablesMap.put(GafferPopGraphVariables.OP_OPTIONS, new String[] {"key1:value1", "key2:value2" });
        variablesMap.put(GafferPopGraphVariables.USER, "user");
        variablesMap.put(GafferPopGraphVariables.SCHEMA, StreamUtil.openStreams(this.getClass(), "/gaffer/schema"));
        return new GafferPopGraphVariables(variablesMap);
    }

}
