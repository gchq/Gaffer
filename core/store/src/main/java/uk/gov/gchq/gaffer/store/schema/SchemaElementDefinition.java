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

import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import uk.gov.gchq.gaffer.data.TransformIterable;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.ElementDefinition;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.function.FilterFunction;
import uk.gov.gchq.gaffer.function.IsA;
import uk.gov.gchq.gaffer.function.context.ConsumerFunctionContext;
import uk.gov.gchq.gaffer.function.context.PassThroughFunctionContext;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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
 */
@JsonFilter(JSONSerialiser.FILTER_FIELDS_BY_NAME)
public abstract class SchemaElementDefinition implements ElementDefinition {
    /**
     * A validator to validate the element definition
     */
    private final SchemaElementDefinitionValidator elementDefValidator;

    /**
     * Property map of property name to accepted type.
     */
    protected LinkedHashMap<String, String> properties;

    /**
     * Identifier map of identifier type to accepted type.
     */
    protected LinkedHashMap<IdentifierType, String> identifiers;

    protected ElementFilter validator;

    /**
     * The <code>TypeDefinitions</code> provides the different element identifier value types and property value types.
     *
     * @see TypeDefinitions
     */
    protected Schema schemaReference;

    /**
     * A ordered set of property names that should be stored to allow
     * query time aggregation to group based on their values.
     */
    protected LinkedHashSet<String> groupBy;

    protected String parentGroup;
    protected String description;

    public SchemaElementDefinition() {
        this.elementDefValidator = new SchemaElementDefinitionValidator();
        properties = new LinkedHashMap<>();
        identifiers = new LinkedHashMap<>();
        groupBy = new LinkedHashSet<>();
        schemaReference = new Schema();
    }

    /**
     * Uses the element definition validator to validate the element definition.
     *
     * @return true if the element definition is valid, otherwise false.
     */
    public boolean validate() {
        return elementDefValidator.validate(this);
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

    public Class<?> getClass(final String key) {
        final Class<?> clazz;
        if (null == key) {
            clazz = null;
        } else {
            final IdentifierType idType = IdentifierType.fromName(key);
            if (null == idType) {
                clazz = getPropertyClass(key);
            } else {
                clazz = getIdentifierClass(idType);
            }
        }

        return clazz;
    }

    /**
     * @return a cloned instance of {@link ElementAggregator} fully populated with all the
     * {@link uk.gov.gchq.gaffer.function.AggregateFunction}s defined in this
     * {@link SchemaElementDefinition} and also the
     * {@link uk.gov.gchq.gaffer.function.AggregateFunction}s defined in the corresponding property value
     * {@link TypeDefinition}s.
     */
    @JsonIgnore
    public ElementAggregator getAggregator() {
        final ElementAggregator aggregator = new ElementAggregator();
        for (final Entry<String, String> entry : getPropertyMap().entrySet()) {
            addTypeAggregateFunctions(aggregator, entry.getKey(), entry.getValue());
        }

        return aggregator;
    }

    /**
     * @return a cloned instance of {@link uk.gov.gchq.gaffer.data.element.function.ElementFilter} fully populated with all the
     * {@link uk.gov.gchq.gaffer.function.FilterFunction}s defined in this
     * {@link SchemaElementDefinition} and also the
     * {@link SchemaElementDefinition} and also the
     * {@link uk.gov.gchq.gaffer.function.FilterFunction}s defined in the corresponding identifier and property value
     * {@link TypeDefinition}s.
     */
    @JsonIgnore
    public ElementFilter getValidator() {
        return getValidator(true);
    }

    public ElementFilter getValidator(final boolean includeIsA) {
        final ElementFilter fullValidator = null != validator ? validator.clone() : new ElementFilter();
        for (final Entry<IdentifierType, String> entry : getIdentifierMap().entrySet()) {
            final String key = entry.getKey().name();
            if (includeIsA) {
                addIsAFunction(fullValidator, key, entry.getValue());
            }
            addTypeValidatorFunctions(fullValidator, key, entry.getValue());
        }
        for (final Entry<String, String> entry : getPropertyMap().entrySet()) {
            final String key = entry.getKey();
            if (includeIsA) {
                addIsAFunction(fullValidator, key, entry.getValue());
            }
            addTypeValidatorFunctions(fullValidator, key, entry.getValue());
        }

        return fullValidator;
    }

    @SuppressFBWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS", justification = "null is only returned when the validator is null")
    @JsonGetter("validateFunctions")
    public ConsumerFunctionContext<String, FilterFunction>[] getOriginalValidateFunctions() {
        if (null != validator) {
            final List<ConsumerFunctionContext<String, FilterFunction>> functions = validator.getFunctions();
            return functions.toArray(new ConsumerFunctionContext[functions.size()]);
        }

        return null;
    }

    @JsonSetter("validateFunctions")
    public void addValidateFunctions(final ConsumerFunctionContext<String, FilterFunction>... functions) {
        if (null == validator) {
            validator = new ElementFilter();
        }
        validator.addFunctions(Arrays.asList(functions));
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

    public String getParentGroup() {
        return parentGroup;
    }

    public String getDescription() {
        return description;
    }

    private void addTypeValidatorFunctions(final ElementFilter fullValidator, final String key, final String classOrTypeName) {
        final TypeDefinition type = getTypeDef(classOrTypeName);
        if (null != type.getValidator()) {
            for (final ConsumerFunctionContext<String, FilterFunction> function : type.getValidator().clone().getFunctions()) {
                final List<String> selection = function.getSelection();
                if (null == selection || selection.isEmpty()) {
                    function.setSelection(Collections.singletonList(key));
                } else if (!selection.contains(key)) {
                    selection.add(key);
                }
                fullValidator.addFunction(function);
            }
        }
    }

    private void addTypeAggregateFunctions(final ElementAggregator aggregator, final String key, final String typeName) {
        final TypeDefinition type = getTypeDef(typeName);
        if (null != type.getAggregateFunction()) {
            aggregator.addFunction(new PassThroughFunctionContext<>(type.getAggregateFunction().statelessClone(), Collections.singletonList(key)));
        }
    }

    private void addIsAFunction(final ElementFilter fullValidator, final String key, final String classOrTypeName) {
        fullValidator.addFunction(
                new ConsumerFunctionContext<String, FilterFunction>(
                        new IsA(getTypeDef(classOrTypeName).getClazz()), Collections.singletonList(key)));
    }

    private TypeDefinition getTypeDef(final String typeName) {
        return schemaReference.getType(typeName);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final SchemaElementDefinition that = (SchemaElementDefinition) o;

        return new EqualsBuilder()
                .append(elementDefValidator, that.elementDefValidator)
                .append(properties, that.properties)
                .append(identifiers, that.identifiers)
                .append(validator, that.validator)
                .append(groupBy, that.groupBy)
                .append(description, that.description)
                .append(parentGroup, that.parentGroup)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(elementDefValidator)
                .append(properties)
                .append(identifiers)
                .append(validator)
                .append(groupBy)
                .append(description)
                .append(parentGroup)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("elementDefValidator", elementDefValidator)
                .append("properties", properties)
                .append("identifiers", identifiers)
                .append("validator", validator)
                .append("groupBy", groupBy)
                .append("description", description)
                .append("parent", parentGroup)
                .toString();
    }

    protected abstract static class BaseBuilder<ELEMENT_DEF extends SchemaElementDefinition,
            CHILD_CLASS extends BaseBuilder<ELEMENT_DEF, ?>> {
        protected ELEMENT_DEF elDef;

        protected BaseBuilder(final ELEMENT_DEF elementDef) {
            this.elDef = elementDef;
        }

        public CHILD_CLASS property(final String propertyName, final String typeName) {
            elDef.properties.put(propertyName, typeName);
            return self();
        }

        public CHILD_CLASS identifier(final IdentifierType identifierType, final String typeName) {
            elDef.identifiers.put(identifierType, typeName);
            return self();
        }

        public CHILD_CLASS validator(final ElementFilter validator) {
            elDef.validator = validator;
            return self();
        }

        public CHILD_CLASS groupBy(final String... propertyName) {
            elDef.getGroupBy().addAll(Arrays.asList(propertyName));
            return self();
        }

        public CHILD_CLASS parent(final String parentGroup) {
            elDef.parentGroup = parentGroup;
            return self();
        }

        public CHILD_CLASS description(final String description) {
            elDef.description = description;
            return self();
        }

        public CHILD_CLASS merge(final SchemaElementDefinition elementDef) {
            for (final Entry<String, String> entry : elementDef.getPropertyMap().entrySet()) {
                final String newProp = entry.getKey();
                final String newPropTypeName = entry.getValue();
                if (!getElementDef().properties.containsKey(newProp)) {
                    getElementDef().properties.put(newProp, newPropTypeName);
                } else {
                    final String typeName = getElementDef().properties.get(newProp);
                    if (!typeName.equals(newPropTypeName)) {
                        throw new SchemaException("Unable to merge schemas. Conflict of types with property " + newProp
                                + ". Type names are: " + typeName + " and " + newPropTypeName);
                    }
                }
            }

            for (final Entry<IdentifierType, String> entry : elementDef.getIdentifierMap().entrySet()) {
                final IdentifierType newId = entry.getKey();
                final String newIdTypeName = entry.getValue();
                if (!getElementDef().identifiers.containsKey(newId)) {
                    getElementDef().identifiers.put(newId, newIdTypeName);
                } else {
                    final String typeName = getElementDef().identifiers.get(newId);
                    if (!typeName.equals(newIdTypeName)) {
                        throw new SchemaException("Unable to merge schemas. Conflict of types with identifier " + newId
                                + ". Type names are: " + typeName + " and " + newIdTypeName);
                    }
                }
            }

            if (null == getElementDef().validator) {
                getElementDef().validator = elementDef.validator;
            } else if (null != elementDef.getOriginalValidateFunctions()) {
                getElementDef().validator.addFunctions(Arrays.asList(elementDef.getOriginalValidateFunctions()));
            }

            getElementDef().groupBy.addAll(elementDef.getGroupBy());

            if (null == getElementDef().description) {
                getElementDef().description = elementDef.getDescription();
            } else if (null != elementDef.getDescription() && !getElementDef().description.contains(elementDef.getDescription())) {
                getElementDef().description = getElementDef().description + " | " + elementDef.getDescription();
            }

            return self();
        }

        public ELEMENT_DEF build() {
            //TODO lock all fields
            return elDef;
        }

        protected abstract CHILD_CLASS self();

        protected ELEMENT_DEF getElementDef() {
            return elDef;
        }
    }
}
