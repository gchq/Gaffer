/*
 * Copyright 2017-2018. Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.query;

import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.api.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;
import uk.gov.gchq.koryphe.impl.predicate.AgeOff;
import uk.gov.gchq.koryphe.impl.predicate.And;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;
import uk.gov.gchq.koryphe.impl.predicate.IsFalse;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;
import uk.gov.gchq.koryphe.impl.predicate.IsTrue;
import uk.gov.gchq.koryphe.impl.predicate.Not;
import uk.gov.gchq.koryphe.impl.predicate.Or;
import uk.gov.gchq.koryphe.tuple.predicate.TupleAdaptedPredicate;

import java.util.List;
import java.util.function.Predicate;

import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.booleanColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.doubleColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.floatColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.gt;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.apache.parquet.filter2.predicate.FilterApi.ltEq;

public class JavaPredicateToParquetPredicate {
    private static final Logger LOGGER = LoggerFactory.getLogger(JavaPredicateToParquetPredicate.class);

    private final SchemaUtils schemaUtils;
    private final Predicate javaPredicate;
    private final String[] selection;
    private final String group;
    private boolean fullyApplied;

    public JavaPredicateToParquetPredicate(final SchemaUtils schemaUtils,
                                           final Predicate javaPredicate,
                                           final String[] selection,
                                           final String group) {
        this.schemaUtils = schemaUtils;
        this.javaPredicate = javaPredicate;
        this.selection = selection;
        this.group = group;
        this.fullyApplied = true;
    }

    public FilterPredicate getParquetPredicate() throws SerialisationException {
        FilterPredicate filterResult;
        if (javaPredicate instanceof AgeOff) {
            filterResult = getAgeOffPredicate((AgeOff) javaPredicate, selection, group, schemaUtils);
        } else if (javaPredicate instanceof And) {
            final And and = (And) javaPredicate;
            filterResult = getAndFilter((List<Predicate>) and.getComponents(), selection, group, schemaUtils);
        } else if (javaPredicate instanceof Or) {
            final Or or = (Or) javaPredicate;
            filterResult = getOrFilter((List<Predicate>) or.getComponents(), selection, group, schemaUtils);
        } else if (javaPredicate instanceof Not) {
            final Not not = (Not) javaPredicate;
            final JavaPredicateToParquetPredicate predicateConverter = new JavaPredicateToParquetPredicate(schemaUtils, not.getPredicate(), selection, group);
            final FilterPredicate parquetPredicate = predicateConverter.getParquetPredicate();
            if (!predicateConverter.fullyApplied) {
                fullyApplied = false;
            }
            filterResult = FilterPredicateUtils.not(parquetPredicate);
        } else {
            filterResult = getPrimitiveFilter(javaPredicate, selection[0], group, schemaUtils);
        }
        return filterResult;
    }


    public FilterPredicate getAgeOffPredicate(final AgeOff ageOff,
                                              final String[] selection,
                                              final String group,
                                              final SchemaUtils schemaUtils) {
        String[] paths = schemaUtils.getPaths(group, selection[0]);
        if (paths == null) {
            paths = new String[1];
            paths[0] = selection[0];
        }
        FilterPredicate filter = null;
        for (int i = 0; i < paths.length; i++) {
            final String path = paths[i];
            FilterPredicate tempFilter;
            Long ageOffTime = System.currentTimeMillis() - ageOff.getAgeOffTime();
            tempFilter = gt(longColumn(path), ageOffTime);
            if (filter == null) {
                filter = tempFilter;
            } else {
                filter = and(filter, tempFilter);
            }
        }
        return filter;
    }

    public FilterPredicate getAndFilter(final List<Predicate> predicateList,
                                               final String[] selection,
                                               final String group,
                                               final SchemaUtils schemaUtils) throws SerialisationException {
        FilterPredicate combinedFilter = null;
        for (final Predicate predicate : predicateList) {
            final Predicate filterFunction;
            final String[] newSelection;
            if (predicate instanceof TupleAdaptedPredicate) {
                filterFunction = ((TupleAdaptedPredicate) predicate).getPredicate();
                // Build new selections
                final Integer[] ints = (Integer[]) ((TupleAdaptedPredicate) predicate).getSelection();
                newSelection = new String[ints.length];
                for (int x = 0; x < ints.length; x++) {
                    newSelection[x] = selection[ints[x]];
                }
            } else {
                filterFunction = predicate;
                newSelection = selection;
            }
            final JavaPredicateToParquetPredicate predicateConverter = new JavaPredicateToParquetPredicate(schemaUtils, filterFunction, newSelection, group);
            final FilterPredicate parquetPredicate = predicateConverter.getParquetPredicate();
            if (!predicateConverter.fullyApplied) {
                fullyApplied = false;
            }
            combinedFilter = FilterPredicateUtils.and(combinedFilter, parquetPredicate);
        }
        return combinedFilter;
    }

    public FilterPredicate getOrFilter(final List<Predicate> predicateList,
                                              final String[] selection,
                                              final String group,
                                              final SchemaUtils schemaUtils) throws SerialisationException {
        FilterPredicate combinedFilter = null;
        for (final Predicate predicate : predicateList) {
            final Predicate filterFunction;
            final String[] newSelection;
            if (predicate instanceof TupleAdaptedPredicate) {
                filterFunction = ((TupleAdaptedPredicate) predicate).getPredicate();
                // Build new selections
                final Integer[] ints = (Integer[]) ((TupleAdaptedPredicate) predicate).getSelection();
                newSelection = new String[ints.length];
                for (int x = 0; x < ints.length; x++) {
                    newSelection[x] = selection[ints[x]];
                }
            } else {
                filterFunction = predicate;
                newSelection = selection;
            }
            final JavaPredicateToParquetPredicate predicateConverter = new JavaPredicateToParquetPredicate(schemaUtils, filterFunction, newSelection, group);
            final FilterPredicate parquetPredicate = predicateConverter.getParquetPredicate();
            if (!predicateConverter.fullyApplied) {
                fullyApplied = false;
            }
            combinedFilter = FilterPredicateUtils.or(combinedFilter, parquetPredicate);
        }
        return combinedFilter;
    }

    public FilterPredicate getPrimitiveFilter(final Predicate filterFunction,
                                              final String selection,
                                              final String group,
                                              final SchemaUtils schemaUtils) throws SerialisationException {
        // All supported filters will be in the if else statement below
        if (filterFunction instanceof IsEqual) {
            final IsEqual isEqual = (IsEqual) filterFunction;
            final Object[] parquetObjects = schemaUtils
                    .getConverter(group)
                    .gafferObjectToParquetObjects(selection, isEqual.getControlValue());

            return getIsEqualFilter(selection, parquetObjects, group, schemaUtils);
        } else if (filterFunction instanceof IsLessThan) {
            final IsLessThan isLessThan = (IsLessThan) filterFunction;
            final Object[] parquetObjects = schemaUtils
                    .getConverter(group)
                    .gafferObjectToParquetObjects(selection, isLessThan.getControlValue());
            if (isLessThan.getOrEqualTo()) {
                return getIsLessThanOrEqualToFilter(selection, parquetObjects, group, schemaUtils);
            }
            return getIsLessThanFilter(selection, parquetObjects, group, schemaUtils);
        } else if (filterFunction instanceof IsMoreThan) {
            final IsMoreThan isMoreThan = (IsMoreThan) filterFunction;
            final Object[] parquetObjects = schemaUtils
                    .getConverter(group)
                    .gafferObjectToParquetObjects(selection, isMoreThan.getControlValue());
            if (isMoreThan.getOrEqualTo()) {
                return getIsMoreThanOrEqualToFilter(selection, parquetObjects, group, schemaUtils);
            }
            return getIsMoreThanFilter(selection, parquetObjects, group, schemaUtils);
        } else if (filterFunction instanceof IsTrue) {
            return eq(booleanColumn(selection), true);
        } else if (filterFunction instanceof IsFalse) {
            return eq(booleanColumn(selection), false);
        } else {
            fullyApplied = false;
            LOGGER.warn(filterFunction.getClass().getCanonicalName() +
                    " is not a natively supported filter by the Parquet store, therefore execution will take longer to perform this filter.");
            return null;
        }
    }

    public FilterPredicate getIsEqualFilter(final String colName,
                                            final Object[] parquetObjects,
                                            final String group,
                                            final SchemaUtils schemaUtils) {
        String[] paths = schemaUtils.getPaths(group, colName);
        if (null == paths) {
            paths = new String[1];
            paths[0] = colName;
        }
        FilterPredicate filter = null;
        for (int i = 0; i < paths.length; i++) {
            final String path = paths[i];
            FilterPredicate tempFilter;
            if (parquetObjects[i] instanceof String) {
                tempFilter = eq(binaryColumn(path), Binary.fromString((String) parquetObjects[i]));
            } else if (parquetObjects[i] instanceof Boolean) {
                tempFilter = eq(booleanColumn(path), (Boolean) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof Double) {
                tempFilter = eq(doubleColumn(path), (Double) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof Float) {
                tempFilter = eq(floatColumn(path), (Float) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof Integer) {
                tempFilter = eq(intColumn(path), (Integer) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof Long) {
                tempFilter = eq(longColumn(path), (Long) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof java.util.Date) {
                tempFilter = eq(longColumn(path), ((java.util.Date) parquetObjects[i]).getTime());
            } else if (parquetObjects[i] instanceof java.sql.Date) {
                tempFilter = eq(longColumn(path), ((java.sql.Date) parquetObjects[i]).getTime());
            } else if (parquetObjects[i] instanceof Short) {
                tempFilter = eq(intColumn(path), ((Short) parquetObjects[i]).intValue());
            } else if (parquetObjects[i] instanceof byte[]) {
                tempFilter = eq(binaryColumn(path), Binary.fromReusedByteArray((byte[]) parquetObjects[i]));
            } else {
                fullyApplied = false;
                LOGGER.warn(parquetObjects[i].getClass().getCanonicalName()
                        + " is not a natively supported type for the IsEqual filter, therefore execution will take longer to perform this filter.");
                return null;
            }
            if (null == filter) {
                filter = tempFilter;
            } else {
                filter = and(filter, tempFilter);
            }
        }
        return filter;
    }

    private FilterPredicate getIsLessThanFilter(final String colName,
                                                final Object[] parquetObjects,
                                                final String group,
                                                final SchemaUtils schemaUtils) {
        String[] paths = schemaUtils.getPaths(group, colName);
        if (null == paths) {
            paths = new String[1];
            paths[0] = colName;
        }
        FilterPredicate filter = null;
        for (int i = 0; i < paths.length; i++) {
            final String path = paths[i];
            FilterPredicate tempFilter;
            if (parquetObjects[i] instanceof String) {
                tempFilter = lt(binaryColumn(path), Binary.fromString((String) parquetObjects[i]));
            } else if (parquetObjects[i] instanceof Double) {
                tempFilter = lt(doubleColumn(path), (Double) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof Float) {
                tempFilter = lt(floatColumn(path), (Float) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof Integer) {
                tempFilter = lt(intColumn(path), (Integer) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof Long) {
                tempFilter = lt(longColumn(path), (Long) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof java.util.Date) {
                tempFilter = lt(longColumn(path), ((java.util.Date) parquetObjects[i]).getTime());
            } else if (parquetObjects[i] instanceof java.sql.Date) {
                tempFilter = lt(longColumn(path), ((java.sql.Date) parquetObjects[i]).getTime());
            } else if (parquetObjects[i] instanceof Short) {
                tempFilter = lt(intColumn(path), ((Short) parquetObjects[i]).intValue());
            } else if (parquetObjects[i] instanceof byte[]) {
                tempFilter = lt(binaryColumn(path), Binary.fromReusedByteArray((byte[]) parquetObjects[i]));
            } else {
                fullyApplied = false;
                LOGGER.warn(parquetObjects[i].getClass().getCanonicalName()
                        + " is not a natively supported type for the IsLessThan filter, therefore execution will take longer to perform this filter.");
                return null;
            }
            if (null == filter) {
                filter = tempFilter;
            } else {
                filter = and(filter, tempFilter);
            }
        }
        return filter;
    }

    private FilterPredicate getIsMoreThanFilter(final String colName,
                                                final Object[] parquetObjects,
                                                final String group,
                                                final SchemaUtils schemaUtils) {
        String[] paths = schemaUtils.getPaths(group, colName);
        if (null == paths) {
            paths = new String[1];
            paths[0] = colName;
        }
        FilterPredicate filter = null;
        for (int i = 0; i < paths.length; i++) {
            final String path = paths[i];
            FilterPredicate tempFilter;
            if (parquetObjects[i] instanceof String) {
                tempFilter = gt(binaryColumn(path), Binary.fromString((String) parquetObjects[i]));
            } else if (parquetObjects[i] instanceof Double) {
                tempFilter = gt(doubleColumn(path), (Double) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof Float) {
                tempFilter = gt(floatColumn(path), (Float) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof Integer) {
                tempFilter = gt(intColumn(path), (Integer) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof Long) {
                tempFilter = gt(longColumn(path), (Long) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof java.util.Date) {
                tempFilter = gt(longColumn(path), ((java.util.Date) parquetObjects[i]).getTime());
            } else if (parquetObjects[i] instanceof java.sql.Date) {
                tempFilter = gt(longColumn(path), ((java.sql.Date) parquetObjects[i]).getTime());
            } else if (parquetObjects[i] instanceof Short) {
                tempFilter = gt(intColumn(path), ((Short) parquetObjects[i]).intValue());
            } else if (parquetObjects[i] instanceof byte[]) {
                tempFilter = gt(binaryColumn(path), Binary.fromReusedByteArray((byte[]) parquetObjects[i]));
            } else {
                fullyApplied = false;
                LOGGER.warn(parquetObjects[i].getClass().getCanonicalName()
                        + " is not a natively supported type for the IsMoreThan filter, therefore execution will take longer to perform this filter.");
                return null;
            }
            if (null == filter) {
                filter = tempFilter;
            } else {
                filter = and(filter, tempFilter);
            }
        }
        return filter;
    }

    private FilterPredicate getIsLessThanOrEqualToFilter(final String colName,
                                                         final Object[] parquetObjects,
                                                         final String group,
                                                         final SchemaUtils schemaUtils) {
        String[] paths = schemaUtils.getPaths(group, colName);
        if (null == paths) {
            paths = new String[1];
            paths[0] = colName;
        }
        FilterPredicate filter = null;
        for (int i = 0; i < paths.length; i++) {
            final String path = paths[i];
            FilterPredicate tempFilter;
            if (parquetObjects[i] instanceof String) {
                tempFilter = ltEq(binaryColumn(path), Binary.fromString((String) parquetObjects[i]));
            } else if (parquetObjects[i] instanceof Double) {
                tempFilter = ltEq(doubleColumn(path), (Double) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof Float) {
                tempFilter = ltEq(floatColumn(path), (Float) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof Integer) {
                tempFilter = ltEq(intColumn(path), (Integer) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof Long) {
                tempFilter = ltEq(longColumn(path), (Long) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof java.util.Date) {
                tempFilter = ltEq(longColumn(path), ((java.util.Date) parquetObjects[i]).getTime());
            } else if (parquetObjects[i] instanceof java.sql.Date) {
                tempFilter = ltEq(longColumn(path), ((java.sql.Date) parquetObjects[i]).getTime());
            } else if (parquetObjects[i] instanceof Short) {
                tempFilter = ltEq(intColumn(path), ((Short) parquetObjects[i]).intValue());
            } else if (parquetObjects[i] instanceof byte[]) {
                tempFilter = ltEq(binaryColumn(path), Binary.fromReusedByteArray((byte[]) parquetObjects[i]));
            } else {
                fullyApplied = false;
                LOGGER.warn(parquetObjects[i].getClass().getCanonicalName()
                        + " is not a natively supported type for the IsLessThanOrEqualTo filter, therefore execution will take longer to perform this filter.");
                return null;
            }
            if (null == filter) {
                filter = tempFilter;
            } else {
                filter = and(filter, tempFilter);
            }
        }
        return filter;
    }

    private FilterPredicate getIsMoreThanOrEqualToFilter(final String colName,
                                                         final Object[] parquetObjects,
                                                         final String group,
                                                         final SchemaUtils schemaUtils) {
        String[] paths = schemaUtils.getPaths(group, colName);
        if (null == paths) {
            paths = new String[1];
            paths[0] = colName;
        }
        FilterPredicate filter = null;
        for (int i = 0; i < paths.length; i++) {
            final String path = paths[i];
            FilterPredicate tempFilter;
            if (parquetObjects[i] instanceof String) {
                tempFilter = gtEq(binaryColumn(path), Binary.fromString((String) parquetObjects[i]));
            } else if (parquetObjects[i] instanceof Double) {
                tempFilter = gtEq(doubleColumn(path), (Double) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof Float) {
                tempFilter = gtEq(floatColumn(path), (Float) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof Integer) {
                tempFilter = gtEq(intColumn(path), (Integer) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof Long) {
                tempFilter = gtEq(longColumn(path), (Long) parquetObjects[i]);
            } else if (parquetObjects[i] instanceof java.util.Date) {
                tempFilter = gtEq(longColumn(path), ((java.util.Date) parquetObjects[i]).getTime());
            } else if (parquetObjects[i] instanceof java.sql.Date) {
                tempFilter = gtEq(longColumn(path), ((java.sql.Date) parquetObjects[i]).getTime());
            } else if (parquetObjects[i] instanceof Short) {
                tempFilter = gtEq(intColumn(path), ((Short) parquetObjects[i]).intValue());
            } else if (parquetObjects[i] instanceof byte[]) {
                tempFilter = gtEq(binaryColumn(path), Binary.fromReusedByteArray((byte[]) parquetObjects[i]));
            } else {
                fullyApplied = false;
                LOGGER.warn(parquetObjects[i].getClass().getCanonicalName()
                        + " is not a natively supported type for the IsMoreThanOrEqualTo filter, therefore execution will take longer to perform this filter.");
                return null;
            }
            if (null == filter) {
                filter = tempFilter;
            } else {
                filter = and(filter, tempFilter);
            }
        }
        return filter;
    }

    public boolean isFullyApplied() {
        return fullyApplied;
    }
}
