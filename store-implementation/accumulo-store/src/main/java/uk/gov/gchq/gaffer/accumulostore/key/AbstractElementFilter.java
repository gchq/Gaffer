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

package uk.gov.gchq.gaffer.accumulostore.key;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import uk.gov.gchq.gaffer.accumulostore.data.element.AccumuloEdgeValueLoader;
import uk.gov.gchq.gaffer.accumulostore.data.element.AccumuloEntityValueLoader;
import uk.gov.gchq.gaffer.accumulostore.key.exception.ElementFilterException;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.ChainedIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.LazyEdge;
import uk.gov.gchq.gaffer.data.element.LazyEntity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.store.ElementValidator;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * The AbstractElementFilter will filter out {@link Element}s based on the filtering
 * instructions given in the {@link Schema} or {@link View} that is passed to this iterator
 */
public abstract class AbstractElementFilter extends Filter {
    protected Schema schema;

    @SuppressFBWarnings(value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR", justification = "elementConverter is initialised in validateOptions method, which is always called first")
    private Predicate<Element> elementPredicate;

    @SuppressFBWarnings(value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR", justification = "elementConverter is initialised in validateOptions method, which is always called first")
    private AccumuloElementConverter elementConverter;

    private Set<String> groupsWithoutFilters = Collections.emptySet();

    private final ElementValidator.FilterType filterType;

    protected AbstractElementFilter(final ElementValidator.FilterType filterType) {
        this.filterType = filterType;
    }

    @Override
    public boolean accept(final Key key, final Value value) {
        final String group = elementConverter.getGroupFromColumnFamily(key.getColumnFamilyData().getBackingArray());
        if (groupsWithoutFilters.contains(group)) {
            return true;
        }

        final Element element;
        if (schema.isEntity(group)) {
            element = new LazyEntity(new Entity(group), new AccumuloEntityValueLoader(group, key, value, elementConverter, schema));
        } else {
            element = new LazyEdge(new Edge(group, null, null, false), new AccumuloEdgeValueLoader(group, key, value, elementConverter, schema, false));
        }
        return elementPredicate.test(element);
    }

    @Override
    public void init(final SortedKeyValueIterator<Key, Value> source,
                     final Map<String, String> options,
                     final IteratorEnvironment env) throws IOException {
        super.init(source, options, env);

        schema = Schema.fromJson(StringUtil.toBytes(options.get(AccumuloStoreConstants.SCHEMA)));

        try {
            elementConverter = Class
                    .forName(options.get(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS))
                    .asSubclass(AccumuloElementConverter.class)
                    .getConstructor(Schema.class)
                    .newInstance(schema);
        } catch (final ClassNotFoundException | InstantiationException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            throw new ElementFilterException("Failed to load element converter from class name provided : "
                    + options.get(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS), e);
        }

        if (filterType == ElementValidator.FilterType.SCHEMA_VALIDATION) {
            updateSchemaGroupsWithoutFilters();
            elementPredicate = new ElementValidator(schema, false)::validateWithSchema;
        } else {
            final String viewJson = options.get(AccumuloStoreConstants.VIEW);
            if (null == viewJson) {
                throw new IllegalArgumentException("Must specify the " + AccumuloStoreConstants.VIEW);
            }
            final View view = View.fromJson(StringUtil.toBytes(viewJson));
            if (filterType == ElementValidator.FilterType.PRE_AGGREGATION_FILTER) {
                updateViewGroupsWithoutFilters(view, ViewElementDefinition::hasPreAggregationFilters);
                elementPredicate = new ElementValidator(view)::validateInput;
            } else {
                updateViewGroupsWithoutFilters(view, ViewElementDefinition::hasPostAggregationFilters);
                elementPredicate = new ElementValidator(view)::validateAggregation;
            }
        }
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(final IteratorEnvironment env) {
        AbstractElementFilter newInstance;
        try {
            newInstance = this.getClass().newInstance();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        newInstance.setSource(getSource().deepCopy(env));
        newInstance.schema = schema;
        newInstance.elementConverter = elementConverter;
        newInstance.elementPredicate = elementPredicate;
        return newInstance;
    }

    @Override
    public boolean validateOptions(final Map<String, String> options) {
        if (!super.validateOptions(options)) {
            return false;
        }
        if (!options.containsKey(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS)) {
            throw new IllegalArgumentException("Must specify the " + AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS);
        }
        if (!options.containsKey(AccumuloStoreConstants.SCHEMA)) {
            throw new IllegalArgumentException("Must specify the " + AccumuloStoreConstants.SCHEMA);
        }

        return true;
    }

    private void updateViewGroupsWithoutFilters(final View view, final Function<ViewElementDefinition, Boolean> hasFilters) {
        groupsWithoutFilters = new HashSet<>();
        for (final Map.Entry<String, ViewElementDefinition> entry : new ChainedIterable<Map.Entry<String, ViewElementDefinition>>(view.getEntities().entrySet(), view.getEdges().entrySet())) {
            if (null == entry.getValue() || !hasFilters.apply(entry.getValue())) {
                groupsWithoutFilters.add(entry.getKey());
            }
        }
    }

    private void updateSchemaGroupsWithoutFilters() {
        groupsWithoutFilters = new HashSet<>();
        for (final Map.Entry<String, SchemaElementDefinition> entry : new ChainedIterable<Map.Entry<String, SchemaElementDefinition>>(schema.getEntities().entrySet(), schema.getEdges().entrySet())) {
            if (null == entry.getValue() || !entry.getValue().hasValidation()) {
                groupsWithoutFilters.add(entry.getKey());
            }
        }
    }
}
