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

package gaffer.store.schema;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import gaffer.data.element.ElementComponentKey;
import gaffer.data.element.IdentifierType;
import gaffer.data.element.function.ElementAggregator;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.elementdefinition.ElementDefinition;
import gaffer.function.FilterFunction;
import gaffer.function.IsA;
import gaffer.function.context.ConsumerFunctionContext;
import gaffer.function.context.PassThroughFunctionContext;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A <code>DataElementDefinition</code> is the representation of a single group in a
 * {@link DataSchema}.
 * Each element needs identifiers and can optionally have properties, an aggregator and a validator.
 *
 * @see DataElementDefinition.Builder
 */
public abstract class DataElementDefinition extends ElementDefinition {
    private ElementFilter validator;

    /**
     * The <code>Types</code> provides the different element identifier value types and property value types.
     *
     * @see Types
     */
    private Types types;

    /**
     * Constructs a <code>DataElementDefinition</code> with a <code>DataElementDefinitionValidator</code> to validate
     * this <code>DataElementDefinition</code>.
     *
     * @see DataElementDefinitionValidator
     */
    public DataElementDefinition() {
        super(new DataElementDefinitionValidator());
    }

    /**
     * @return a cloned instance of {@link ElementAggregator} fully populated with all the
     * {@link gaffer.function.AggregateFunction}s defined in this
     * {@link DataElementDefinition} and also the
     * {@link gaffer.function.AggregateFunction}s defined in the corresponding property value
     * {@link Type}s.
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
     * @return a cloned instance of {@link ElementFilter} fully populated with all the
     * {@link FilterFunction}s defined in this
     * {@link DataElementDefinition} and also the
     * {@link FilterFunction}s defined in the corresponding identifier and property value
     * {@link Type}s.
     */
    public ElementFilter getValidator() {
        final ElementFilter fullValidator = null != validator ? validator.clone() : new ElementFilter();
        for (Map.Entry<IdentifierType, String> entry : getIdentifierMap().entrySet()) {
            final ElementComponentKey key = new ElementComponentKey(entry.getKey());
            addIsAFunction(fullValidator, key, entry.getValue());
            addTypeValidatorFunctions(fullValidator, key, entry.getValue());
        }
        for (Map.Entry<String, String> entry : getPropertyMap().entrySet()) {
            final ElementComponentKey key = new ElementComponentKey(entry.getKey());
            addIsAFunction(fullValidator, key, entry.getValue());
            addTypeValidatorFunctions(fullValidator, key, entry.getValue());
        }

        return fullValidator;
    }

    private void setValidator(final ElementFilter validator) {
        this.validator = validator;
    }

    @JsonProperty("validator")
    ElementFilter getOriginalValidator() {
        return validator;
    }

    @JsonIgnore
    public Types getTypes() {
        if (null == types) {
            setTypes(new Types());
        }

        return types;
    }

    public void setTypes(final Types newTypes) {
        if (null != types && null != newTypes && types != newTypes) {
            newTypes.putAll(types);
        }

        types = newTypes;
    }

    public Type getType(final String typeName) {
        return getTypes().getType(typeName);
    }

    public Type getProperty(final String property) {
        return getType(getPropertyMap().get(property));
    }

    public Type getIdentifier(final IdentifierType idType) {
        return getType(getIdentifierMap().get(idType));
    }

    @Override
    public Class<?> getPropertyClass(final String propertyName) {
        final String typeName = super.getPropertyTypeName(propertyName);
        return null != typeName ? getType(typeName).getClazz() : null;
    }

    @Override
    public Class<?> getIdentifierClass(final IdentifierType idType) {
        final String typeName = super.getIdentifierTypeName(idType);
        return null != typeName ? getType(typeName).getClazz() : null;
    }

    private void addTypeValidatorFunctions(final ElementFilter fullValidator, final ElementComponentKey key, final String classOrTypeName) {
        final Type type = getType(classOrTypeName);
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
        final Type type = getType(typeName);
        if (null != type.getAggregateFunction()) {
            aggregator.addFunction(new PassThroughFunctionContext<>(type.getAggregateFunction().statelessClone(), Collections.singletonList(key)));
        }
    }

    private void addIsAFunction(final ElementFilter fullValidator, final ElementComponentKey key, final String classOrTypeName) {
        fullValidator.addFunction(
                new ConsumerFunctionContext<ElementComponentKey, FilterFunction>(
                        new IsA(getType(classOrTypeName).getClazz()), Collections.singletonList(key)));
    }

    protected static class Builder extends ElementDefinition.Builder {
        protected Builder(final DataElementDefinition elDef) {
            super(elDef);
        }

        protected Builder validator(final ElementFilter validator) {
            getElementDef().setValidator(validator);
            return this;
        }

        protected Builder property(final String propertyName, final String typeName, final Type type) {
            type(typeName, type);
            return (Builder) property(propertyName, typeName);
        }

        public Builder property(final String propertyName, final String typeName, final Class<?> typeClass) {
            return property(propertyName, typeName, new Type(typeClass));
        }

        protected Builder type(final String typeName, final Type type) {
            final Types types = getElementDef().getTypes();
            final Type exisitingType;
            try {
                exisitingType = types.getType(typeName);
                if (!exisitingType.equals(type)) {
                    throw new IllegalArgumentException("The type provided conflicts with an existing type with the same name");
                }
            } catch (final IllegalArgumentException e) {
                types.put(typeName, type);
            }

            return this;
        }

        protected DataElementDefinition build() {
            return (DataElementDefinition) super.build();
        }

        protected DataElementDefinition getElementDef() {
            return (DataElementDefinition) super.getElementDef();
        }
    }
}
