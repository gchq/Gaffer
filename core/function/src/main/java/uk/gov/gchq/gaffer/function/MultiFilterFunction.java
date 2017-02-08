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

package uk.gov.gchq.gaffer.function;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import uk.gov.gchq.gaffer.function.context.ConsumerFunctionContext;
import uk.gov.gchq.gaffer.function.processor.Filter;
import java.util.ArrayList;
import java.util.InputMismatchException;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

/**
 * A <code>MultiFilterFunction</code> is a {@link FilterFunction} that
 * contains a list of {@link FilterFunction}s. This can be used to create an
 * And, Or and other complex filter functions.
 */
public abstract class MultiFilterFunction extends FilterFunction {
    private final Filter<Integer> filter;

    public MultiFilterFunction() {
        this(new ArrayList<ConsumerFunctionContext<Integer, FilterFunction>>());
    }

    public MultiFilterFunction(final List<ConsumerFunctionContext<Integer, FilterFunction>> functions) {
        this.filter = new Filter<>(functions);
    }

    public List<ConsumerFunctionContext<Integer, FilterFunction>> getFunctions() {
        return filter.getFunctions();
    }

    public void setFunctions(final List<ConsumerFunctionContext<Integer, FilterFunction>> functions) {
        filter.addFunctions(functions);
    }

    @Override
    public Class<?>[] getInputClasses() {
        final TreeMap<Integer, Class<?>> inputClassMap = new TreeMap<>();
        for (final ConsumerFunctionContext<Integer, FilterFunction> context : getFunctions()) {
            final Class<?>[] inputClasses = context.getFunction().getInputClasses();
            for (int i = 0; i < inputClasses.length; i++) {
                final Integer index = context.getSelection().get(i);
                if (inputClassMap.containsKey(index)) {
                    final Class<?> otherClazz = inputClassMap.get(index);
                    if (otherClazz.isAssignableFrom(inputClasses[i])) {
                        inputClassMap.put(index, inputClasses[i]);
                    } else if (!inputClasses[i].isAssignableFrom(otherClazz)) {
                        throw new InputMismatchException("Input types for function " + getClass().getSimpleName() + " are not compatible");
                    }
                } else {
                    inputClassMap.put(index, inputClasses[i]);
                }
            }
        }

        return inputClassMap.values().toArray(new Class<?>[inputClassMap.size()]);
    }

    protected List<ConsumerFunctionContext<Integer, FilterFunction>> cloneFunctions() {
        return filter.clone().getFunctions();
    }

    protected Iterable<Boolean> executeFilters(final Object[] input) {
        return new Iterable<Boolean>() {
            @Override
            public Iterator<Boolean> iterator() {
                final Iterator<ConsumerFunctionContext<Integer, FilterFunction>> funcItr = getFunctions().iterator();
                return new Iterator<Boolean>() {
                    @Override
                    public boolean hasNext() {
                        return funcItr.hasNext();
                    }

                    @Override
                    public Boolean next() {
                        return executeFunction(input, funcItr.next());
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException("Cannot remove from items from this iterator");
                    }

                    private boolean executeFunction(final Object[] input, final ConsumerFunctionContext<Integer, FilterFunction> function) {
                        final Object[] selection;
                        if (null == function.getSelection()) {
                            selection = input;
                        } else {
                            selection = function.select(new ArrayTuple(input));
                        }

                        return function.getFunction().isValid(selection);
                    }
                };
            }
        };
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final MultiFilterFunction that = (MultiFilterFunction) o;

        return new EqualsBuilder()
                .appendSuper(super.equals(o))
                .append(inputs, that.inputs)
                .append(filter, that.filter)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .appendSuper(super.hashCode())
                .append(inputs)
                .append(filter)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("inputs", inputs)
                .append("filter", filter)
                .toString();
    }
}
