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

import org.apache.tinkerpop.gremlin.jsr223.Customizer;
import org.apache.tinkerpop.gremlin.jsr223.GremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.tinkerpop.gremlinplugin.GafferPopGremlinPlugin;
import uk.gov.gchq.koryphe.impl.function.Concat;
import uk.gov.gchq.koryphe.impl.predicate.Exists;

import java.util.Arrays;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class GafferPopGremlinPluginTest {

    @Test
    void shouldCreateGafferPopGremlinPlugin() {
        // Given
        final GafferPopGremlinPlugin plugin = GafferPopGremlinPlugin.instance();

        // Then
        assertThat(plugin.getName()).isEqualTo("gafferpop");
        assertThat(plugin).isInstanceOf(GremlinPlugin.class);
    }

    @Test
    void shouldRequireRestart() {
        // Given
        final GafferPopGremlinPlugin plugin = GafferPopGremlinPlugin.instance();

        // Then
        assertThat(plugin.requireRestart()).isTrue();
    }

    @Test
    void shouldHaveSpecificCustomImports() {
        // Given
        final GafferPopGremlinPlugin plugin = GafferPopGremlinPlugin.instance();

        // When
        final Optional<Customizer[]> customizers = plugin.getCustomizers();
        assertThat(customizers).isPresent();

        Optional<ImportCustomizer> importCustomizer = Arrays.stream(customizers.get())
                .filter(ImportCustomizer.class::isInstance)
                .map(customizer -> (ImportCustomizer) customizer)
                .findFirst();
        assertThat(importCustomizer).isPresent();

        // Then
        assertThat(importCustomizer.get().getClassImports())
                .hasSize(8);
        assertThat(importCustomizer.get().getClassImports())
                .contains(GafferPopGraph.class,
                        View.class,
                        ViewElementDefinition.class,
                        ElementFilter.class,
                        ElementTransformer.class,
                        IdentifierType.class,
                        Exists.class,
                        Concat.class);
    }
}
