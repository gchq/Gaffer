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
    protected Map<String, String> properties;

    /**
     * Identifier map of identifier type to accepted type.
     */
    protected Map<IdentifierType, String> identifiers;

    protected ElementFilter validator;

    protected Schema schemaReference;

    /**
     * A ordered set of property names that should be stored to allow
     * query time aggregation to group based on their values.
     */
    protected Set<String> groupBy;
    protected Set<String> parents;
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
     * @param requiresAggregators true if aggregators are required
     * @return true if the element definition is valid, otherwise false.
     */
    public boolean validate(final boolean requiresAggregators) {
        return elementDefValidator.validate(this, requiresAggregators);
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

    public Set<String> getGroupBy() {
        return groupBy;
    }

    protected Set<String> getParents() {
        return parents;
    }

    /**
     * For json serialisation if there are no parents then just return null
     *
     * @return parents
     */
    @JsonGetter("parents")
    protected Set<String> getParentsOrNull() {
        if (null == parents || parents.isEmpty()) {
            return null;
        }

        return parents;
    }

    public String getDescription() {
        return description;
    }

    @JsonIgnore
    public abstract SchemaElementDefinition getExpandedDefinition();

    protected Schema getSchemaReference() {
        return schemaReference;
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
                .toString();
    }

    @Override
    public void lock() {
        if (null != parents) {
            parents = Collections.unmodifiableSet(parents);
        }
        groupBy = Collections.unmodifiableSet(groupBy);
        properties = Collections.unmodifiableMap(properties);
        identifiers = Collections.unmodifiableMap(identifiers);
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

        public CHILD_CLASS properties(final Map<String, String> properties) {
            if (null == elDef.properties) {
                elDef.properties = new LinkedHashMap<>(properties);
            } else {
                elDef.properties.putAll(properties);
            }
            return self();
        }

        public CHILD_CLASS identifier(final IdentifierType identifierType, final String typeName) {
            elDef.identifiers.put(identifierType, typeName);
            return self();
        }

        public CHILD_CLASS identifiers(final Map<IdentifierType, String> identifiers) {
            if (null == elDef.identifiers) {
                elDef.identifiers = new LinkedHashMap<>(identifiers);
            } else {
                elDef.identifiers.putAll(identifiers);
            }
            return self();
        }

        public CHILD_CLASS validator(final ElementFilter validator) {
            elDef.validator = validator;
            return self();
        }

        public CHILD_CLASS validateFunctions(final List<ConsumerFunctionContext<String, FilterFunction>> filterFunctions) {
            if (null == getElementDef().validator) {
                getElementDef().validator = new ElementFilter();
            }
            getElementDef().validator.addFunctions(filterFunctions);
            return self();
        }

        public CHILD_CLASS groupBy(final String... propertyName) {
            Collections.addAll(elDef.getGroupBy(), propertyName);
            return self();
        }

        public CHILD_CLASS parents(final String... parents) {
            if (parents.length > 0) {
                if (null == elDef.parents) {
                    elDef.parents = new LinkedHashSet<>();
                }

                Collections.addAll(elDef.parents, parents);
            }
            return self();
        }

        public CHILD_CLASS description(final String description) {
            elDef.description = description;
            return self();
        }

        public CHILD_CLASS merge(final ELEMENT_DEF elementDef) {
            if (getElementDef().properties.isEmpty()) {
                getElementDef().properties.putAll(elementDef.getPropertyMap());
            } else {
                for (final Entry<String, String> entry : elementDef.getPropertyMap().entrySet()) {
                    if (null == getElementDef().getPropertyTypeName(entry.getKey())) {
                        getElementDef().properties.put(entry.getKey(), entry.getValue());
                    } else {
                        throw new SchemaException("Unable to merge element definitions because the property exists in both definitions");
                    }
                }
            }

            if (getElementDef().identifiers.isEmpty()) {
                getElementDef().identifiers.putAll(elementDef.getIdentifierMap());
            } else {
                for (final Entry<IdentifierType, String> entry : elementDef.getIdentifierMap().entrySet()) {
                    getElementDef().identifiers.put(entry.getKey(), entry.getValue());
                }
            }

            if (null == getElementDef().validator) {
                getElementDef().validator = elementDef.validator;
            } else if (null != elementDef.getOriginalValidateFunctions()) {
                getElementDef().validator.addFunctions(Arrays.asList(elementDef.getOriginalValidateFunctions()));
            }

            getElementDef().groupBy = new LinkedHashSet<>(elementDef.groupBy);
            getElementDef().parents = null != elementDef.parents ? new LinkedHashSet<>(elementDef.parents) : null;
            getElementDef().description = elementDef.description;

            return self();
        }

        public ELEMENT_DEF build() {
            elDef.lock();
            return elDef;
        }

        protected abstract CHILD_CLASS self();

        protected ELEMENT_DEF getElementDef() {
            return elDef;
        }
    }
}
