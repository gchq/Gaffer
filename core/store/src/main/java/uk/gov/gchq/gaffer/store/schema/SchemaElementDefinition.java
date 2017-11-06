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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.google.common.collect.Sets;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.commonutil.PropertiesUtil;
import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.commonutil.iterable.TransformIterable;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.ElementDefinition;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.ValidationResult;
import uk.gov.gchq.koryphe.impl.predicate.IsA;
import uk.gov.gchq.koryphe.tuple.Tuple;
import uk.gov.gchq.koryphe.tuple.binaryoperator.TupleAdaptedBinaryOperator;
import uk.gov.gchq.koryphe.tuple.predicate.TupleAdaptedPredicate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Predicate;

/**
 * A {@code SchemaElementDefinition} is the representation of a single group in a
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
     * Map of property name to accepted type name.
     * The type name relates to the types part of the schema
     */
    protected Map<String, String> properties;

    /**
     * Identifier map of identifier type to accepted type.
     */
    protected Map<IdentifierType, String> identifiers;

    protected ElementFilter validator;

    protected ElementFilter fullValidatorCache;

    protected ElementFilter fullValidatorWithIsACache;

    protected ElementAggregator aggregator;

    protected Set<String> propertiesInAggregatorCache;

    protected ElementAggregator fullAggregatorCache;

    protected ElementAggregator ingestAggregatorCache;

    protected final Map<Set<String>, ElementAggregator> queryAggregatorCacheMap = new HashMap<>();

    protected Schema schemaReference;

    /**
     * A ordered set of property names that should be stored to allow
     * query time aggregation to group based on their values.
     */
    protected Set<String> groupBy;
    protected Set<String> parents;
    protected String description;
    protected boolean aggregate = true;

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
    public ValidationResult validate() {
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

    @JsonIgnore
    public ElementAggregator getOriginalAggregator() {
        return aggregator;
    }

    @JsonGetter("aggregateFunctions")
    public List<TupleAdaptedBinaryOperator<String, ?>> getOriginalAggregateFunctions() {
        return null != aggregator ? aggregator.getComponents() : null;
    }

    @JsonIgnore
    public ElementAggregator getFullAggregator() {
        if (null == fullAggregatorCache) {
            fullAggregatorCache = new ElementAggregator();
            if (aggregate) {
                if (null != aggregator) {
                    fullAggregatorCache.getComponents().addAll(aggregator.getComponents());
                }
                final Set<String> aggregatorProperties = getAggregatorProperties();
                for (final Entry<String, String> entry : getPropertyMap().entrySet()) {
                    if (!aggregatorProperties.contains(entry.getKey())) {
                        addTypeAggregateFunction(fullAggregatorCache, entry.getKey(), entry.getValue());
                    }
                }
            }
            fullAggregatorCache.lock();
        }

        return fullAggregatorCache;
    }

    @JsonIgnore
    public ElementAggregator getIngestAggregator() {
        if (null == ingestAggregatorCache) {
            ingestAggregatorCache = new ElementAggregator();
            if (aggregate) {
                final Set<String> aggregatorProperties = getAggregatorProperties();
                if (null != aggregator) {
                    for (final TupleAdaptedBinaryOperator<String, ?> component : aggregator.getComponents()) {
                        final String[] selection = component.getSelection();
                        if (selection.length == 1 && !groupBy.contains(selection[0]) && !selection[0].equals(schemaReference.getVisibilityProperty())) {
                            ingestAggregatorCache.getComponents().add(component);
                        } else if (!CollectionUtil.containsAny(groupBy, selection)) {
                            ingestAggregatorCache.getComponents().add(component);
                        }
                    }
                }
                for (final Entry<String, String> entry : getPropertyMap().entrySet()) {
                    if (!aggregatorProperties.contains(entry.getKey())) {
                        if (!groupBy.contains(entry.getKey()) && !entry.getKey().equals(schemaReference.getVisibilityProperty())) {
                            addTypeAggregateFunction(ingestAggregatorCache, entry.getKey(), entry.getValue());
                        }
                    }
                }
            }
            ingestAggregatorCache.lock();
        }

        return ingestAggregatorCache;
    }

    @JsonIgnore
    public ElementAggregator getQueryAggregator(final Set<String> viewGroupBy, final ElementAggregator viewAggregator) {
        ElementAggregator queryAggregator = null;
        if (null == viewAggregator) {
            queryAggregator = queryAggregatorCacheMap.get(viewGroupBy);
        }

        if (null == queryAggregator) {
            queryAggregator = new ElementAggregator();
            if (aggregate) {
                final Set<String> mergedGroupBy = null == viewGroupBy ? groupBy : viewGroupBy;
                final Set<String> viewAggregatorProps;
                if (null == viewAggregator) {
                    viewAggregatorProps = Collections.emptySet();
                } else {
                    int size = getPropertyMap().size() - mergedGroupBy.size();
                    if (size < 0) {
                        size = 0;
                    }
                    viewAggregatorProps = new HashSet<>(size);
                    for (final TupleAdaptedBinaryOperator<String, ?> component : viewAggregator.getComponents()) {
                        Collections.addAll(viewAggregatorProps, component.getSelection());
                        queryAggregator.getComponents().add(component);
                    }
                }
                if (null == aggregator) {
                    for (final Entry<String, String> entry : getPropertyMap().entrySet()) {
                        if (!mergedGroupBy.contains(entry.getKey()) && !viewAggregatorProps.contains(entry.getKey())) {
                            addTypeAggregateFunction(queryAggregator, entry.getKey(), entry.getValue());
                        }
                    }
                } else {
                    for (final TupleAdaptedBinaryOperator<String, ?> component : aggregator.getComponents()) {
                        final String[] selection = component.getSelection();
                        if (selection.length == 1 && !mergedGroupBy.contains(selection[0]) && !viewAggregatorProps.contains(selection[0])) {
                            queryAggregator.getComponents().add(component);
                        } else if (CollectionUtil.anyMissing(mergedGroupBy, selection) && CollectionUtil.anyMissing(viewAggregatorProps, selection)) {
                            queryAggregator.getComponents().add(component);
                        }
                    }
                    final Set<String> aggregatorProperties = getAggregatorProperties();
                    for (final Entry<String, String> entry : getPropertyMap().entrySet()) {
                        if (!mergedGroupBy.contains(entry.getKey()) && !viewAggregatorProps.contains(entry.getKey()) && !aggregatorProperties.contains(entry.getKey())) {
                            addTypeAggregateFunction(queryAggregator, entry.getKey(), entry.getValue());
                        }
                    }
                }
            }
            queryAggregator.lock();
            // Don't cache the aggregator if a view aggregator has been provided
            if (null == viewAggregator) {
                queryAggregatorCacheMap.put(viewGroupBy, queryAggregator);
            }
        }

        return queryAggregator;
    }

    @JsonIgnore
    public ElementFilter getValidator() {
        return getValidator(true);
    }

    @JsonIgnore
    public ElementFilter getOriginalValidator() {
        return validator;
    }

    public boolean hasValidation() {
        if (null != validator && !validator.getComponents().isEmpty()) {
            return true;
        }

        final Set<String> typeNames = Sets.newHashSet(identifiers.values());
        typeNames.addAll(properties.values());
        for (final String typeName : typeNames) {
            final TypeDefinition typeDef = getTypeDef(typeName);
            if (null != typeDef) {
                if (null != typeDef.getValidateFunctions() && !typeDef.getValidateFunctions().isEmpty()) {
                    return true;
                }
            }
        }

        return false;
    }

    public ElementFilter getValidator(final boolean includeIsA) {
        ElementFilter fullValidatorTmp;
        if (includeIsA) {
            fullValidatorTmp = fullValidatorWithIsACache;
        } else {
            fullValidatorTmp = fullValidatorCache;
        }

        if (null == fullValidatorTmp) {
            fullValidatorTmp = new ElementFilter();
            if (null != validator) {
                fullValidatorTmp.setComponents(new ArrayList<>(validator.getComponents()));
            }
            for (final Entry<IdentifierType, String> entry : getIdentifierMap().entrySet()) {
                final String key = entry.getKey().name();
                if (includeIsA) {
                    addIsAFunction(fullValidatorTmp, key, entry.getValue());
                }
                addTypeValidatorFunctions(fullValidatorTmp, key, entry.getValue());
            }
            for (final Entry<String, String> entry : getPropertyMap().entrySet()) {
                final String key = entry.getKey();
                if (includeIsA) {
                    addIsAFunction(fullValidatorTmp, key, entry.getValue());
                }
                addTypeValidatorFunctions(fullValidatorTmp, key, entry.getValue());
            }

            fullValidatorTmp.lock();
            if (includeIsA) {
                fullValidatorWithIsACache = fullValidatorTmp;
            } else {
                fullValidatorCache = fullValidatorTmp;
            }
        }
        return fullValidatorTmp;
    }

    @SuppressFBWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS", justification = "null is only returned when the validator is null")
    @JsonGetter("validateFunctions")
    public TupleAdaptedPredicate[] getOriginalValidateFunctions() {
        if (null != validator) {
            final List<TupleAdaptedPredicate<String, ?>> functions = validator.getComponents();
            return functions.toArray(new TupleAdaptedPredicate[functions.size()]);
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
        if (null != type.getValidateFunctions()) {
            for (final Predicate<?> predicate : type.getValidateFunctions()) {
                fullValidator.getComponents().add(new TupleAdaptedPredicate<>(predicate, new String[]{key}));
            }
        }
    }

    private void addTypeAggregateFunction(final ElementAggregator aggregator, final String key, final String typeName) {
        final TypeDefinition type = getTypeDef(typeName);
        if (null != type.getAggregateFunction()) {
            aggregator.getComponents().add(new TupleAdaptedBinaryOperator<>(type.getAggregateFunction(), new String[]{key}));
        }
    }

    private void addIsAFunction(final ElementFilter fullValidator, final String key, final String classOrTypeName) {
        fullValidator.getComponents().add(
                new TupleAdaptedPredicate<>(
                        new IsA(getTypeDef(classOrTypeName).getClazz()), new String[]{key}));
    }

    private TypeDefinition getTypeDef(final String typeName) {
        return schemaReference.getType(typeName);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final SchemaElementDefinition that = (SchemaElementDefinition) obj;

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
        return new HashCodeBuilder(73, 41)
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
        if (null != validator) {
            validator.lock();
        }
        if (null != aggregator) {
            aggregator.lock();
        }
    }

    @JsonInclude(value = JsonInclude.Include.NON_DEFAULT)
    public boolean isAggregate() {
        return aggregate;
    }

    public void setAggregate(final boolean aggregate) {
        this.aggregate = aggregate;
    }

    private Set<String> getAggregatorProperties() {
        if (null == propertiesInAggregatorCache) {
            if (null == aggregator) {
                propertiesInAggregatorCache = Collections.emptySet();
            } else {
                propertiesInAggregatorCache = new HashSet<>();
                for (final TupleAdaptedBinaryOperator<String, ?> component : aggregator.getComponents()) {
                    Collections.addAll(propertiesInAggregatorCache, component.getSelection());
                }
            }
        }
        return propertiesInAggregatorCache;
    }

    protected abstract static class BaseBuilder<ELEMENT_DEF extends SchemaElementDefinition,
            CHILD_CLASS extends BaseBuilder<ELEMENT_DEF, ?>> {
        protected final ELEMENT_DEF elDef;

        protected BaseBuilder(final ELEMENT_DEF elementDef) {
            this.elDef = elementDef;
        }

        public CHILD_CLASS property(final String propertyName, final String typeName) {
            elDef.properties.put(propertyName, typeName);
            return self();
        }

        public CHILD_CLASS properties(final Map<String, String> properties) {
            if (null == properties) {
                elDef.properties = null;
            } else if (null == elDef.properties) {
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
            if (null == identifiers) {
                elDef.identifiers = null;
            } else if (null == elDef.identifiers) {
                elDef.identifiers = new LinkedHashMap<>(identifiers);
            } else {
                elDef.identifiers.putAll(identifiers);
            }
            return self();
        }

        public CHILD_CLASS aggregator(final ElementAggregator aggregator) {
            elDef.aggregator = aggregator;
            return self();
        }

        @JsonSetter("aggregateFunctions")
        public CHILD_CLASS aggregateFunctions(final List<TupleAdaptedBinaryOperator<String, Tuple<String>>> aggregateFunctions) {
            if (null != aggregateFunctions) {
                if (null == elDef.aggregator) {
                    elDef.aggregator = new ElementAggregator();
                }
                elDef.aggregator.getComponents().addAll(aggregateFunctions);
            }
            return self();
        }

        public CHILD_CLASS validator(final ElementFilter validator) {
            elDef.validator = validator;
            return self();
        }

        @JsonSetter("validateFunctions")
        public CHILD_CLASS validateFunctions(final List<TupleAdaptedPredicate<String, Tuple<String>>> predicates) {
            if (null != predicates) {
                if (null == elDef.validator) {
                    elDef.validator = new ElementFilter();
                }
                elDef.validator.getComponents().addAll(predicates);
            }
            return self();
        }

        @SafeVarargs
        public final CHILD_CLASS validateFunctions(final TupleAdaptedPredicate<String, Tuple<String>>... predicates) {
            if (null != predicates) {
                if (null == elDef.validator) {
                    elDef.validator = new ElementFilter();
                }
                Collections.addAll(elDef.validator.getComponents(), predicates);
            }
            return self();
        }

        public final CHILD_CLASS validateFunctions(final ElementFilter elementFilter) {
            if (null != elementFilter) {
                validateFunctions((List<TupleAdaptedPredicate<String, Tuple<String>>>) (List) elementFilter.getComponents());
            }
            return self();
        }

        public CHILD_CLASS groupBy(final String... propertyName) {
            if (null != propertyName) {
                Collections.addAll(elDef.getGroupBy(), propertyName);
            }
            return self();
        }

        public CHILD_CLASS parents(final String... parents) {
            if (null != parents && parents.length > 0) {
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

        public CHILD_CLASS aggregate(final boolean aggregate) {
            elDef.aggregate = aggregate;
            return self();
        }

        public CHILD_CLASS merge(final ELEMENT_DEF elementDef) {
            if (null != elementDef) {
                if (elDef.properties.isEmpty()) {
                    elDef.properties.putAll(elementDef.getPropertyMap());
                } else {
                    for (final Entry<String, String> entry : elementDef.getPropertyMap().entrySet()) {
                        final String typeName = elDef.getPropertyTypeName(entry.getKey());
                        if (null == typeName) {
                            elDef.properties.put(entry.getKey(), entry.getValue());
                        } else if (!typeName.equals(entry.getValue())) {
                            throw new SchemaException("Unable to merge element definitions because the property " + entry.getKey() + " exists in both definitions with different types: " + typeName + " and " + entry.getValue());
                        }
                    }
                }

                if (elDef.identifiers.isEmpty()) {
                    elDef.identifiers.putAll(elementDef.getIdentifierMap());
                } else {
                    for (final Entry<IdentifierType, String> entry : elementDef.getIdentifierMap().entrySet()) {
                        elDef.identifiers.put(entry.getKey(), entry.getValue());
                    }
                }

                if (null == elDef.validator) {
                    elDef.validator = elementDef.validator;
                } else if (null != elementDef.getOriginalValidateFunctions()) {
                    final ElementFilter combinedFilter = new ElementFilter();
                    combinedFilter.getComponents().addAll(elDef.validator.getComponents());
                    combinedFilter.getComponents().addAll(elementDef.validator.getComponents());
                    combinedFilter.lock();
                    elDef.validator = combinedFilter;
                }
                elDef.fullValidatorCache = null;
                elDef.fullValidatorWithIsACache = null;

                if (null == elDef.aggregator) {
                    elDef.aggregator = elementDef.aggregator;
                } else if (null != elementDef.getOriginalAggregateFunctions()) {
                    final ElementAggregator combinedAggregator = new ElementAggregator();
                    combinedAggregator.getComponents().addAll(elDef.aggregator.getComponents());
                    combinedAggregator.getComponents().addAll(elementDef.aggregator.getComponents());
                    combinedAggregator.lock();
                    elDef.aggregator = combinedAggregator;
                }
                elDef.propertiesInAggregatorCache = null;
                elDef.fullAggregatorCache = null;
                elDef.ingestAggregatorCache = null;
                elDef.queryAggregatorCacheMap.clear();

                if (null != elementDef.groupBy && !elementDef.groupBy.isEmpty()) {
                    elDef.groupBy = new LinkedHashSet<>(elementDef.groupBy);
                }

                if (null != elementDef.parents && !elementDef.parents.isEmpty()) {
                    elDef.parents = new LinkedHashSet<>(elementDef.parents);
                }

                if (null != elementDef.description) {
                    elDef.description = elementDef.description;
                }

                elDef.aggregate = elDef.aggregate && elementDef.aggregate;
            }
            return self();
        }

        public ELEMENT_DEF build() {
            elDef.getProperties().forEach(PropertiesUtil::validateName);
            elDef.lock();
            return elDef;
        }

        protected abstract CHILD_CLASS self();

        protected ELEMENT_DEF getElementDef() {
            return elDef;
        }
    }
}
