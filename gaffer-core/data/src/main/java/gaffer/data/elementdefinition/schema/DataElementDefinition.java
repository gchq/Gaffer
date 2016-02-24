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

package gaffer.data.elementdefinition.schema;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import gaffer.data.element.ElementComponentKey;
import gaffer.data.element.IdentifierType;
import gaffer.data.element.function.ElementAggregator;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.elementdefinition.Type;
import gaffer.data.elementdefinition.TypeStore;
import gaffer.data.elementdefinition.TypedElementDefinition;
import gaffer.function.FilterFunction;
import gaffer.function.IsA;
import gaffer.function.context.ConsumerFunctionContext;
import gaffer.function.context.PassThroughFunctionContext;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A <code>DataElementDefinition</code> is the representation of a single group in a
 * {@link gaffer.data.elementdefinition.schema.DataSchema}.
 * Each element needs identifiers and can optionally have properties, an aggregator and a validator.
 *
 * @see gaffer.data.elementdefinition.schema.DataElementDefinition.Builder
 * @see gaffer.data.elementdefinition.TypedElementDefinition
 */
public abstract class DataElementDefinition extends TypedElementDefinition {
    private ElementFilter validator;
    private ElementAggregator aggregator;

    /**
     * The <code>TypeStore</code> provides the different element identifier value types and property value types.
     *
     * @see gaffer.data.elementdefinition.TypeStore
     */
    private TypeStore typeStore;

    /**
     * Constructs a <code>DataElementDefinition</code> with a <code>DataElementDefinitionValidator</code> to validate
     * this <code>DataElementDefinition</code>.
     *
     * @see gaffer.data.elementdefinition.schema.DataElementDefinitionValidator
     */
    public DataElementDefinition() {
        super(new DataElementDefinitionValidator());
    }

    /**
     * @return a cloned instance of {@link gaffer.data.element.function.ElementAggregator} fully populated with all the
     * {@link gaffer.function.AggregateFunction}s defined in this
     * {@link gaffer.data.elementdefinition.schema.DataElementDefinition} and also the
     * {@link gaffer.function.AggregateFunction}s defined in the corresponding property value
     * {@link gaffer.data.elementdefinition.Type}s.
     */
    public ElementAggregator getAggregator() {
        final ElementAggregator fullAggregator = null != aggregator ? aggregator.clone() : new ElementAggregator();
        for (Map.Entry<String, String> entry : getPropertyMap().entrySet()) {
            addTypeAggregatorFunctions(fullAggregator, new ElementComponentKey(entry.getKey()), entry.getValue());
        }

        return fullAggregator;
    }

    public void setAggregator(final ElementAggregator aggregator) {
        this.aggregator = aggregator;
    }

    /**
     * @return a cloned instance of {@link gaffer.data.element.function.ElementFilter} fully populated with all the
     * {@link gaffer.function.FilterFunction}s defined in this
     * {@link gaffer.data.elementdefinition.schema.DataElementDefinition} and also the
     * {@link gaffer.function.FilterFunction}s defined in the corresponding identifier and property value
     * {@link gaffer.data.elementdefinition.Type}s.
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

    public void setValidator(final ElementFilter validator) {
        this.validator = validator;
    }

    @JsonProperty("validator")
    ElementFilter getOriginalValidator() {
        return validator;
    }

    @JsonProperty("aggregator")
    ElementAggregator getOriginalAggregator() {
        return aggregator;
    }

    @JsonIgnore
    public TypeStore getTypeStore() {
        return typeStore;
    }

    public void setTypeStore(final TypeStore typeStore) {
        this.typeStore = typeStore;
    }

    @Override
    public String getIdentifierClassName(final IdentifierType idType) {
        final String classOrTypeName = super.getIdentifierClassName(idType);
        final Type type = getType(classOrTypeName);
        return null != type ? type.getClassString() : classOrTypeName;
    }

    @Override
    public String getPropertyClassName(final String propertyName) {
        final String classOrTypeName = super.getPropertyClassName(propertyName);
        final Type type = getType(classOrTypeName);
        return null != type ? type.getClassString() : classOrTypeName;
    }

    public Class<?> getClassFromClassOrTypeName(final String classOrTypeName) {
        final Type type = getType(classOrTypeName);
        return null != type ? type.getClazz() : getClass(classOrTypeName);
    }

    private Type getType(final String classOrTypeName) {
        return null != typeStore ? typeStore.get(classOrTypeName) : null;
    }

    private void addTypeValidatorFunctions(final ElementFilter fullValidator, final ElementComponentKey key, final String classOrTypeName) {
        final Type type = getType(classOrTypeName);
        if (null != type && null != type.getValidator()) {
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

    private void addTypeAggregatorFunctions(final ElementAggregator aggregator, final ElementComponentKey key, final String typeName) {
        final Type type = getType(typeName);
        if (null != type && null != type.getAggregatorFunction()) {
            aggregator.addFunction(new PassThroughFunctionContext<>(type.getAggregatorFunction().statelessClone(), Collections.singletonList(key)));
        }
    }

    private void addIsAFunction(final ElementFilter fullValidator, final ElementComponentKey key, final String classOrTypeName) {
        fullValidator.addFunction(
                new ConsumerFunctionContext<ElementComponentKey, FilterFunction>(
                        new IsA(getClassFromClassOrTypeName(classOrTypeName)), Collections.singletonList(key)));
    }

    protected static class Builder extends TypedElementDefinition.Builder {
        public Builder(final DataElementDefinition elDef) {
            super(elDef);
        }

        public Builder validator(final ElementFilter validator) {
            getElementDef().setValidator(validator);
            return this;
        }

        public Builder aggregator(final ElementAggregator aggregator) {
            getElementDef().setAggregator(aggregator);
            return this;
        }

        @Override
        protected DataElementDefinition getElementDef() {
            return (DataElementDefinition) super.getElementDef();
        }
    }
}
