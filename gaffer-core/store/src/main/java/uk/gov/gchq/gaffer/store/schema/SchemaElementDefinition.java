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

package uk.gov.gchq.gaffer.store.schema;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import uk.gov.gchq.gaffer.data.TransformIterable;
import uk.gov.gchq.gaffer.data.element.ElementComponentKey;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.ElementDefinition;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.function.FilterFunction;
import uk.gov.gchq.gaffer.function.IsA;
import uk.gov.gchq.gaffer.function.context.ConsumerFunctionContext;
import uk.gov.gchq.gaffer.function.context.PassThroughFunctionContext;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * A <code>SchemaElementDefinition</code> is the representation of a single group in a
 * {@link Schema}.
 * Each element needs identifiers and can optionally have properties, an aggregator and a validator.
 *
 * @see SchemaElementDefinition.Builder
 */
public abstract class SchemaElementDefinition implements ElementDefinition {
    private static final Map<String, Class<?>> CLASSES = new HashMap<>();

    /**
     * A validator to validate the element definition
     */
    private final SchemaElementDefinitionValidator elementDefValidator;

    /**
     * Property map of property name to accepted type.
     */
    private LinkedHashMap<String, String> properties;

    /**
     * Identifier map of identifier type to accepted type.
     */
    private LinkedHashMap<IdentifierType, String> identifiers;

    private ElementFilter validator;

    /**
     * The <code>TypeDefinitions</code> provides the different element identifier value types and property value types.
     *
     * @see TypeDefinitions
     */
    private TypeDefinitions typesLookup;

    /**
     * A ordered set of property names that should be stored to allow
     * query time aggregation to group based on their values.
     */
    private LinkedHashSet<String> groupBy;

    public SchemaElementDefinition() {
        this.elementDefValidator = new SchemaElementDefinitionValidator();
        properties = new LinkedHashMap<>();
        identifiers = new LinkedHashMap<>();
        groupBy = new LinkedHashSet<>();
    }

    /**
     * Uses the element definition validator to validate the element definition.
     *
     * @return true if the element definition is valid, otherwise false.
     */
    public boolean validate() {
        return elementDefValidator.validate(this);
    }

    @Override
    public void merge(final ElementDefinition elementDef) {
        if (elementDef instanceof SchemaElementDefinition) {
            merge(((SchemaElementDefinition) elementDef));
        } else {
            throw new IllegalArgumentException("Cannot merge a schema element definition with a " + elementDef.getClass());
        }
    }

    public void merge(final SchemaElementDefinition elementDef) {
        for (Entry<String, String> entry : elementDef.getPropertyMap().entrySet()) {
            final String newProp = entry.getKey();
            final String newPropTypeName = entry.getValue();
            if (!properties.containsKey(newProp)) {
                properties.put(newProp, newPropTypeName);
            } else {
                final String typeName = properties.get(newProp);
                if (!typeName.equals(newPropTypeName)) {
                    throw new SchemaException("Unable to merge schemas. Conflict of types with property " + newProp
                            + ". Type names are: " + typeName + " and " + newPropTypeName);
                }
            }
        }

        for (Entry<IdentifierType, String> entry : elementDef.getIdentifierMap().entrySet()) {
            final IdentifierType newId = entry.getKey();
            final String newIdTypeName = entry.getValue();
            if (!identifiers.containsKey(newId)) {
                identifiers.put(newId, newIdTypeName);
            } else {
                final String typeName = identifiers.get(newId);
                if (!typeName.equals(newIdTypeName)) {
                    throw new SchemaException("Unable to merge schemas. Conflict of types with identifier " + newId
                            + ". Type names are: " + typeName + " and " + newIdTypeName);
                }
            }
        }

        if (null == validator) {
            validator = elementDef.validator;
        } else if (null != elementDef.getOriginalValidateFunctions()) {
            validator.addFunctions(Arrays.asList(elementDef.getOriginalValidateFunctions()));
        }

        if (typesLookup == null) {
            typesLookup = elementDef.getTypesLookup();
        } else if (typesLookup != elementDef.getTypesLookup()) {
            typesLookup.merge(elementDef.getTypesLookup());
        }

        groupBy.addAll(elementDef.getGroupBy());
    }

    public Set<String> getProperties() {
        return properties.keySet();
    }

    public boolean containsProperty(final String propertyName) {
        return properties.containsKey(propertyName);
    }


    @JsonGetter("properties")
    public Map<String, String> getPropertyMap() {
        return Collections.unmodifiableMap(properties);
    }

    @JsonSetter("properties")
    protected void setPropertyMap(final LinkedHashMap<String, String> properties) {
        this.properties = properties;
    }

    @JsonIgnore
    public Collection<IdentifierType> getIdentifiers() {
        return identifiers.keySet();
    }

    @JsonIgnore
    public Map<IdentifierType, String> getIdentifierMap() {
        return identifiers;
    }

    public boolean containsIdentifier(final IdentifierType identifierType) {
        return identifiers.containsKey(identifierType);
    }

    public String getPropertyTypeName(final String propertyName) {
        return properties.get(propertyName);
    }

    public String getIdentifierTypeName(final IdentifierType idType) {
        return identifiers.get(idType);
    }

    @JsonIgnore
    public Collection<String> getPropertyTypeNames() {
        return properties.values();
    }

    @JsonIgnore
    public Collection<String> getIdentifierTypeNames() {
        return identifiers.values();
    }

    public Class<?> getClass(final ElementComponentKey key) {
        if (key.isId()) {
            return getIdentifierClass(key.getIdentifierType());
        }

        return getPropertyClass(key.getPropertyName());
    }

    public Class<?> getClass(final String className) {
        if (null == className) {
            return null;
        }

        Class<?> clazz = CLASSES.get(className);
        if (null == clazz) {
            try {
                clazz = Class.forName(className);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Class could not be found: " + className, e);
            }
            CLASSES.put(className, clazz);
        }

        return clazz;
    }

    /**
     * @return a cloned instance of {@link ElementAggregator} fully populated with all the
     * {@link gaffer.function.AggregateFunction}s defined in this
     * {@link SchemaElementDefinition} and also the
     * {@link gaffer.function.AggregateFunction}s defined in the corresponding property value
     * {@link TypeDefinition}s.
     */
    @JsonIgnore
    public ElementAggregator getAggregator() {
        final ElementAggregator aggregator = new ElementAggregator();
        for (Map.Entry<String, String> entry : getPropertyMap().entrySet()) {
            addTypeAggregateFunctions(aggregator, new ElementComponentKey(entry.getKey()), entry.getValue());
        }

        return aggregator;
    }

    /**
     * @return a cloned instance of {@link gaffer.data.element.function.ElementFilter} fully populated with all the
     * {@link gaffer.function.FilterFunction}s defined in this
     * {@link SchemaElementDefinition} and also the
     * {@link SchemaElementDefinition} and also the
     * {@link gaffer.function.FilterFunction}s defined in the corresponding identifier and property value
     * {@link TypeDefinition}s.
     */
    @JsonIgnore
    public ElementFilter getValidator() {
        return getValidator(true);
    }

    public ElementFilter getValidator(final boolean includeIsA) {
        final ElementFilter fullValidator = null != validator ? validator.clone() : new ElementFilter();
        for (Map.Entry<IdentifierType, String> entry : getIdentifierMap().entrySet()) {
            final ElementComponentKey key = new ElementComponentKey(entry.getKey());
            if (includeIsA) {
                addIsAFunction(fullValidator, key, entry.getValue());
            }
            addTypeValidatorFunctions(fullValidator, key, entry.getValue());
        }
        for (Map.Entry<String, String> entry : getPropertyMap().entrySet()) {
            final ElementComponentKey key = new ElementComponentKey(entry.getKey());
            if (includeIsA) {
                addIsAFunction(fullValidator, key, entry.getValue());
            }
            addTypeValidatorFunctions(fullValidator, key, entry.getValue());
        }

        return fullValidator;
    }

    @JsonSetter("validator")
    private void setValidator(final ElementFilter validator) {
        this.validator = validator;
    }

    @SuppressFBWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS", justification = "null is only returned when the validator is null")
    @JsonGetter("validateFunctions")
    public ConsumerFunctionContext<ElementComponentKey, FilterFunction>[] getOriginalValidateFunctions() {
        if (null != validator) {
            final List<ConsumerFunctionContext<ElementComponentKey, FilterFunction>> functions = validator.getFunctions();
            return functions.toArray(new ConsumerFunctionContext[functions.size()]);
        }

        return null;
    }

    @JsonSetter("validateFunctions")
    public void addValidateFunctions(final ConsumerFunctionContext<ElementComponentKey, FilterFunction>... functions) {
        if (null == validator) {
            validator = new ElementFilter();
        }
        validator.addFunctions(Arrays.asList(functions));
    }

    public void setTypesLookup(final TypeDefinitions newTypes) {
        if (null != typesLookup && null != newTypes) {
            newTypes.merge(typesLookup);
        }

        typesLookup = newTypes;
    }

    @JsonIgnore
    public Iterable<TypeDefinition> getPropertyTypeDefs() {
        return new TransformIterable<String, TypeDefinition>(getPropertyMap().values()) {
            @Override
            public void close() {

            }

            @Override
            protected TypeDefinition transform(final String typeName) {
                return getTypeDef(typeName);
            }
        };
    }

    public TypeDefinition getPropertyTypeDef(final String property) {
        if (containsProperty(property)) {
            return getTypeDef(getPropertyMap().get(property));
        }

        return null;
    }

    public Class<?> getPropertyClass(final String propertyName) {
        final String typeName = getPropertyTypeName(propertyName);
        return null != typeName ? getTypeDef(typeName).getClazz() : null;
    }

    public Class<?> getIdentifierClass(final IdentifierType idType) {
        final String typeName = getIdentifierTypeName(idType);
        return null != typeName ? getTypeDef(typeName).getClazz() : null;
    }

    public LinkedHashSet<String> getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(final LinkedHashSet<String> groupBy) {
        if (null == groupBy) {
            this.groupBy = new LinkedHashSet<>();
        } else {
            this.groupBy = groupBy;
        }
    }

    @JsonIgnore
    protected TypeDefinitions getTypesLookup() {
        if (null == typesLookup) {
            setTypesLookup(new TypeDefinitions());
        }

        return typesLookup;
    }

    private void addTypeValidatorFunctions(final ElementFilter fullValidator, final ElementComponentKey key, final String classOrTypeName) {
        final TypeDefinition type = getTypeDef(classOrTypeName);
        if (null != type.getValidator()) {
            for (ConsumerFunctionContext<ElementComponentKey, FilterFunction> function : type.getValidator().clone().getFunctions()) {
                final List<ElementComponentKey> selection = function.getSelection();
                if (null == selection || selection.isEmpty()) {
                    function.setSelection(Collections.singletonList(key));
                } else if (!selection.contains(key)) {
                    selection.add(key);
                }
                fullValidator.addFunction(function);
            }
        }
    }

    private void addTypeAggregateFunctions(final ElementAggregator aggregator, final ElementComponentKey key, final String typeName) {
        final TypeDefinition type = getTypeDef(typeName);
        if (null != type.getAggregateFunction()) {
            aggregator.addFunction(new PassThroughFunctionContext<>(type.getAggregateFunction().statelessClone(), Collections.singletonList(key)));
        }
    }

    private void addIsAFunction(final ElementFilter fullValidator, final ElementComponentKey key, final String classOrTypeName) {
        fullValidator.addFunction(
                new ConsumerFunctionContext<ElementComponentKey, FilterFunction>(
                        new IsA(getTypeDef(classOrTypeName).getClazz()), Collections.singletonList(key)));
    }

    private TypeDefinition getTypeDef(final String typeName) {
        return getTypesLookup().getType(typeName);
    }

    protected static class Builder {
        private final SchemaElementDefinition elDef;

        protected Builder(final SchemaElementDefinition elDef) {
            this.elDef = elDef;
        }

        protected Builder property(final String propertyName, final String typeName) {
            elDef.properties.put(propertyName, typeName);
            return this;
        }

        protected Builder identifier(final IdentifierType identifierType, final String typeName) {
            elDef.identifiers.put(identifierType, typeName);
            return this;
        }

        protected Builder validator(final ElementFilter validator) {
            elDef.setValidator(validator);
            return this;
        }

        protected Builder property(final String propertyName, final Class<?> clazz) {
            return property(propertyName, clazz.getName(), clazz);
        }

        protected Builder property(final String propertyName, final String typeName, final TypeDefinition type) {
            type(typeName, type);
            return property(propertyName, typeName);
        }

        protected Builder property(final String propertyName, final String typeName, final Class<?> typeClass) {
            return property(propertyName, typeName, new TypeDefinition(typeClass));
        }

        protected Builder type(final String typeName, final TypeDefinition type) {
            final TypeDefinitions types = getElementDef().getTypesLookup();
            final TypeDefinition exisitingType = types.get(typeName);
            if (null == exisitingType) {
                types.put(typeName, type);
            } else if (!exisitingType.equals(type)) {
                throw new IllegalArgumentException("The type provided conflicts with an existing type with the same name");
            }

            return this;
        }

        protected Builder groupBy(final String... propertyName) {
            elDef.getGroupBy().addAll(Arrays.asList(propertyName));
            return this;
        }

        protected SchemaElementDefinition build() {
            return elDef;
        }

        protected SchemaElementDefinition getElementDef() {
            return elDef;
        }
    }
}
