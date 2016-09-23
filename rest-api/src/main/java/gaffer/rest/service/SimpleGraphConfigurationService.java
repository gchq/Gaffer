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

package gaffer.rest.service;

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import gaffer.data.generator.ElementGenerator;
import gaffer.function.FilterFunction;
import gaffer.function.TransformFunction;
import gaffer.operation.Operation;
import gaffer.rest.GraphFactory;
import gaffer.rest.SystemProperty;
import gaffer.store.StoreTrait;
import gaffer.store.schema.Schema;
import org.apache.commons.lang.StringUtils;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * An implementation of {@link gaffer.rest.service.IGraphConfigurationService}. By default it will use a singleton
 * {@link gaffer.graph.Graph} generated using the {@link gaffer.rest.GraphFactory}.
 * <p>
 * Currently the {@link gaffer.operation.Operation}s, {@link gaffer.function.FilterFunction}s,
 * {@link gaffer.function.TransformFunction}s and {@link gaffer.data.generator.ElementGenerator}s available
 * are only returned if they are in a package prefixed with 'gaffer'.
 */
public class SimpleGraphConfigurationService implements IGraphConfigurationService {
    private static final List<Class> FILTER_FUNCTIONS = getSubClasses(FilterFunction.class);
    private static final List<Class> TRANSFORM_FUNCTIONS = getSubClasses(TransformFunction.class);
    private static final List<Class> GENERATORS = getSubClasses(ElementGenerator.class);

    private final GraphFactory graphFactory;

    public SimpleGraphConfigurationService() {
        this(GraphFactory.createGraphFactory());
    }

    public SimpleGraphConfigurationService(final GraphFactory graphFactory) {
        this.graphFactory = graphFactory;
    }

    public static void initialise() {
        // Invoking this method will cause the static lists to be populated.
    }

    @Override
    public Schema getSchema() {
        return graphFactory.getGraph().getSchema();
    }

    @Override
    public List<Class> getFilterFunctions() {
        return FILTER_FUNCTIONS;
    }

    @SuppressFBWarnings(value = "REC_CATCH_EXCEPTION", justification = "Need to wrap all runtime exceptions before they are given to the user")
    @Override
    public List<Class> getFilterFunctions(final String inputClass) {
        if (StringUtils.isEmpty(inputClass)) {
            return getFilterFunctions();
        }

        final Class<?> clazz;
        try {
            clazz = Class.forName(inputClass);
        } catch (Exception e) {
            throw new IllegalArgumentException("Input class was not recognised: " + inputClass, e);
        }

        final List<Class> classes = new ArrayList<>();
        for (final Class functionClass : FILTER_FUNCTIONS) {
            try {
                final FilterFunction function = (FilterFunction) functionClass.newInstance();
                final Class<?>[] inputs = function.getInputClasses();
                if (inputs.length == 1 && inputs[0].isAssignableFrom(clazz)) {
                    classes.add(functionClass);
                }
            } catch (final Exception e) {
                // just add the function.
                classes.add(functionClass);
            }
        }

        return classes;
    }

    @SuppressFBWarnings(value = "REC_CATCH_EXCEPTION", justification = "Need to wrap all runtime exceptions before they are given to the user")
    @Override
    public List<String> getSerialisedFields(final String className) {
        final Class<?> clazz;
        try {
            clazz = Class.forName(className);
        } catch (Exception e) {
            throw new IllegalArgumentException("Class name was not recognised: " + className, e);
        }

        final ObjectMapper mapper = new ObjectMapper();
        final JavaType type = mapper.getTypeFactory().constructType(clazz);
        final BeanDescription introspection = mapper.getSerializationConfig().introspect(type);
        final List<BeanPropertyDefinition> properties = introspection.findProperties();

        final List<String> fields = new ArrayList<>();
        for (BeanPropertyDefinition property : properties) {
            fields.add(property.getName());
        }

        return fields;
    }

    @Override
    public List<Class> getTransformFunctions() {
        return TRANSFORM_FUNCTIONS;
    }

    @Override
    public Set<StoreTrait> getStoreTraits() {
        return graphFactory.getGraph().getStoreTraits();
    }

    @Override
    public List<Class> getGenerators() {
        return GENERATORS;
    }

    @Override
    public Set<Class<? extends Operation>> getOperations() {
        return graphFactory.getGraph().getSupportedOperations();
    }

    @Override
    public Boolean isOperationSupported(final Class<? extends Operation> operation) {
        return graphFactory.getGraph().isSupported(operation);
    }

    private static List<Class> getSubClasses(final Class<?> clazz) {
        final Set<URL> urls = new HashSet<>();
        for (String packagePrefix : System.getProperty(SystemProperty.PACKAGE_PREFIXES, SystemProperty.PACKAGE_PREFIXES_DEFAULT).split(",")) {
            urls.addAll(ClasspathHelper.forPackage(packagePrefix));
        }

        final List<Class> classes = new ArrayList<Class>(new Reflections(urls).getSubTypesOf(clazz));
        keepPublicConcreteClasses(classes);
        Collections.sort(classes, new Comparator<Class>() {
            @Override
            public int compare(final Class class1, final Class class2) {
                return class1.getName().compareTo(class2.getName());
            }
        });

        return classes;

    }

    private static void keepPublicConcreteClasses(final Collection<Class> classes) {
        if (null != classes) {
            final Iterator<Class> itr = classes.iterator();
            while (itr.hasNext()) {
                final Class clazz = itr.next();
                if (null != clazz) {
                    final int modifiers = clazz.getModifiers();
                    if (Modifier.isAbstract(modifiers) || Modifier.isInterface(modifiers) || Modifier.isPrivate(modifiers) || Modifier.isProtected(modifiers)) {
                        itr.remove();
                    }
                }
            }
        }
    }
}
