/*
 * Copyright 2016-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.tinkerpop.gremlinplugin;

import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer;

import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;
import uk.gov.gchq.koryphe.impl.function.Concat;
import uk.gov.gchq.koryphe.impl.predicate.Exists;

public final class GafferPopGremlinPlugin extends AbstractGremlinPlugin {
    private static final String NAME = "gafferpop";
    private static final ImportCustomizer IMPORTS = createImports();
    private static final GafferPopGremlinPlugin INSTANCE = new GafferPopGremlinPlugin();

    public GafferPopGremlinPlugin() {
        super(NAME, IMPORTS);
    }

    public static GafferPopGremlinPlugin instance() {
        return INSTANCE;
    }

    @Override
    public boolean requireRestart() {
        return true;
    }

    private static DefaultImportCustomizer createImports() {
        return DefaultImportCustomizer.build()
                .addClassImports(
                        GafferPopGraph.class,
                        View.class,
                        ViewElementDefinition.class,
                        ElementFilter.class,
                        ElementTransformer.class,
                        IdentifierType.class,
                        Exists.class,
                        Concat.class
                ).create();
    }
}
