/*
 * Copyright 2016 Crown Copyright
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
package gaffer.gafferpop.groovyplugin;

import gaffer.data.element.ElementComponentKey;
import gaffer.data.element.IdentifierType;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.element.function.ElementTransformer;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewElementDefinition;
import gaffer.function.simple.filter.Exists;
import gaffer.function.simple.transform.Concat;
import gaffer.gafferpop.GafferPopGraph;
import org.apache.tinkerpop.gremlin.groovy.plugin.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.groovy.plugin.IllegalEnvironmentException;
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginInitializationException;
import java.util.HashSet;
import java.util.Set;

public final class GafferPopGremlinPlugin extends AbstractGremlinPlugin {
    private static final Set<String> IMPORTS = new HashSet<String>() {
        {
            add(getPackage(GafferPopGraph.class));
            add(getPackage(View.class));
            add(getPackage(ViewElementDefinition.class));
            add(getPackage(ElementFilter.class));
            add(getPackage(ElementTransformer.class));
            add(getPackage(ElementComponentKey.class));
            add(getPackage(IdentifierType.class));
            add(getPackage(Exists.class));
            add(getPackage(Concat.class));
        }
    };

    private static String getPackage(final Class<?> clazz) {
        return IMPORT_SPACE + clazz.getPackage().getName() + DOT_STAR;
    }

    @Override
    public String getName() {
        return GafferPopGraph.class.getName();
    }

    @Override
    public void pluginTo(final PluginAcceptor pluginAcceptor) throws PluginInitializationException, IllegalEnvironmentException {
        pluginAcceptor.addImports(IMPORTS);
    }

    @Override
    public void afterPluginTo(final PluginAcceptor pluginAcceptor) throws IllegalEnvironmentException, PluginInitializationException {

    }
}
