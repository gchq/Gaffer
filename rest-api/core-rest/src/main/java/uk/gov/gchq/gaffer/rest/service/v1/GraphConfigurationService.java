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

package uk.gov.gchq.gaffer.rest.service.v1;

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.StringUtils;

import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.data.generator.ElementGenerator;
import uk.gov.gchq.gaffer.data.generator.ObjectGenerator;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.rest.SystemProperty;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.factory.UserFactory;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.GetTraits;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.serialisation.json.SimpleClassNameIdResolver;
import uk.gov.gchq.koryphe.signature.Signature;
import uk.gov.gchq.koryphe.util.ReflectionUtil;

import javax.inject.Inject;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * An implementation of {@link IGraphConfigurationService}. By default it will use a singleton
 * {@link uk.gov.gchq.gaffer.graph.Graph} generated using the {@link uk.gov.gchq.gaffer.rest.factory.GraphFactory}.
 * <p>
 * Currently the {@link uk.gov.gchq.gaffer.operation.Operation}s, {@link java.util.function.Predicate}s,
 * {@link java.util.function.Function}s and {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator}s available
 * are only returned if they are in a package prefixed with 'gaffer'.
 */
public class GraphConfigurationService implements IGraphConfigurationService {
    @Inject
    private GraphFactory graphFactory;

    @Inject
    private UserFactory userFactory;

    public GraphConfigurationService() {
        updateReflectionPaths();
    }

    public static void initialise() {
        // Invoking this method will cause the static lists to be populated.
        updateReflectionPaths();
    }

    private static void updateReflectionPaths() {
        ReflectionUtil.addReflectionPackages(System.getProperty(SystemProperty.PACKAGE_PREFIXES, SystemProperty.PACKAGE_PREFIXES_DEFAULT));
    }

    @Override
    public Schema getSchema() {
        return graphFactory.getGraph().getSchema();
    }

    @Override
    public String getDescription() {
        return graphFactory.getGraph().getDescription();
    }

    @Override
    public Set<Class> getFilterFunctions() {
        return ReflectionUtil.getSubTypes(Predicate.class);
    }

    @SuppressFBWarnings(value = "REC_CATCH_EXCEPTION", justification = "Need to wrap all runtime exceptions before they are given to the user")
    @Override
    public Set<Class> getFilterFunctions(final String inputClass) {
        if (StringUtils.isEmpty(inputClass)) {
            return getFilterFunctions();
        }

        final Class<?> clazz;
        try {
            clazz = Class.forName(SimpleClassNameIdResolver.getClassName(inputClass));
        } catch (final Exception e) {
            throw new IllegalArgumentException("Input class was not recognised: " + inputClass, e);
        }

        final Set<Class> classes = new HashSet<>();
        for (final Class functionClass : ReflectionUtil.getSubTypes(Predicate.class)) {
            try {
                final Predicate function = (Predicate) functionClass.newInstance();
                final Signature signature = Signature.getInputSignature(function);
                if (null == signature.getNumClasses()
                        || (1 == signature.getNumClasses() &&
                        (Signature.UnknownGenericType.class.isAssignableFrom(signature.getClasses()[0])
                                || signature.getClasses()[0].isAssignableFrom(clazz)))) {
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
    public Set<String> getSerialisedFields(final String className) {
        final Class<?> clazz;
        try {
            clazz = Class.forName(SimpleClassNameIdResolver.getClassName(className));
        } catch (final Exception e) {
            throw new IllegalArgumentException("Class name was not recognised: " + className, e);
        }

        final ObjectMapper mapper = new ObjectMapper();
        final JavaType type = mapper.getTypeFactory().constructType(clazz);
        final BeanDescription introspection = mapper.getSerializationConfig().introspect(type);
        final List<BeanPropertyDefinition> properties = introspection.findProperties();

        final Set<String> fields = new HashSet<>();
        for (final BeanPropertyDefinition property : properties) {
            fields.add(property.getName());
        }

        return fields;
    }

    @Override
    public Set<Class> getTransformFunctions() {
        return ReflectionUtil.getSubTypes(Function.class);
    }

    @Override
    public Set<StoreTrait> getStoreTraits() {
        try {
            return graphFactory.getGraph().execute(new GetTraits.Builder().currentTraits(false).build(), userFactory.createContext());
        } catch (final OperationException e) {
            throw new GafferRuntimeException("Unable to get Traits using GetTraits Operation", e);
        }
    }

    @Override
    public Set<Class> getNextOperations(final String operationClassName) {
        Class<? extends Operation> opClass;
        try {
            opClass = Class.forName(SimpleClassNameIdResolver.getClassName(operationClassName)).asSubclass(Operation.class);
        } catch (final ClassNotFoundException e) {
            throw new IllegalArgumentException("Operation class was not found: " + operationClassName, e);
        } catch (final ClassCastException e) {
            throw new IllegalArgumentException(operationClassName + " does not extend Operation", e);
        }

        return (Set) graphFactory.getGraph().getNextOperations(opClass);
    }

    @Override
    public Set<Class> getElementGenerators() {
        return ReflectionUtil.getSubTypes(ElementGenerator.class);
    }

    @Override
    public Set<Class> getObjectGenerators() {
        return ReflectionUtil.getSubTypes(ObjectGenerator.class);
    }

    @Override
    public Set<Class> getOperations() {
        return (Set) graphFactory.getGraph().getSupportedOperations();
    }

    @Override
    public Boolean isOperationSupported(final Class operation) {
        return graphFactory.getGraph().isSupported(operation);
    }
}
