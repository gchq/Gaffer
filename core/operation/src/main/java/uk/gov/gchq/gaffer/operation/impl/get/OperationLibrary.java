/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.impl.get;

import com.google.common.collect.Lists;
import org.apache.commons.collections.map.SingletonMap;

import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.named.view.AddNamedView;
import uk.gov.gchq.gaffer.named.view.DeleteNamedView;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.CountGroups;
import uk.gov.gchq.gaffer.operation.impl.GetWalks;
import uk.gov.gchq.gaffer.operation.impl.If;
import uk.gov.gchq.gaffer.operation.impl.While;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.compare.Sort;
import uk.gov.gchq.gaffer.operation.impl.export.resultcache.ExportToGafferResultCache;
import uk.gov.gchq.gaffer.operation.impl.export.set.ExportToSet;
import uk.gov.gchq.gaffer.operation.impl.function.Aggregate;
import uk.gov.gchq.gaffer.operation.impl.function.Filter;
import uk.gov.gchq.gaffer.operation.impl.output.ToCsv;
import uk.gov.gchq.gaffer.operation.impl.output.ToSet;
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiElementIdInput;
import uk.gov.gchq.gaffer.operation.io.MultiEntityIdInput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.operation.util.AggregatePair;
import uk.gov.gchq.gaffer.operation.util.OperationUtil;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static uk.gov.gchq.gaffer.data.elementdefinition.view.View.createView;

public class OperationLibrary {
    public static OperationChain<Void> chain() {
        return new OperationChain<>();
    }

    public static OpBuilder input(final Object... input) {
        return OpBuilder.input(input);
    }

    public static GetAdjacentIds hop(final String... edges) {
        return getAdjacentIds(edges);
    }

    public static GetAdjacentIds getAdjacentIds(final String... edges) {
        return new GetAdjacentIds().view(createView().edges(edges));
    }

    public static GetWalks getWalks() {
        return new GetWalks();
    }

    public static GetWalks walk() {
        return getWalks();
    }

    public static GetElements get() {
        return getElements();
    }

    public static GetAllElements getAll() {
        return getAllElements();
    }

    public static AddElements addElements() {
        return new AddElements();
    }

    public static AddNamedView addNamedView(final String name, final View view, final List<String> writeAccessRoles) {
        return new AddNamedView.Builder().name(name).view(view).writeAccessRoles(writeAccessRoles).build();
    }

    public static AddNamedView addNamedView(final String name, final View view, final String... writeAccessRoles) {
        return new AddNamedView.Builder().name(name).view(view).writeAccessRoles(Lists.newArrayList(writeAccessRoles)).build();
    }

    public static Aggregate aggregate() {
        return new Aggregate();
    }

    public static Aggregate aggregateEdge(final String group, final AggregatePair pair) {
        return new Aggregate.Builder().edge(group, pair).build();
    }

    public static Aggregate aggregateEntity(final String group, final AggregatePair pair) {
        return new Aggregate.Builder().entity(group, pair).build();
    }

    public static Aggregate aggregateEdges(final Map<String, AggregatePair> edges) {
        return new Aggregate.Builder().edges(edges).build();
    }

    public static Aggregate aggregateEntities(final Map<String, AggregatePair> entities) {
        return new Aggregate.Builder().entities(entities).build();
    }

    public static CountGroups countGroups() {
        return new CountGroups();
    }

    public static DeleteNamedView deleteNamedView(final String viewName) {
        return new DeleteNamedView.Builder().name(viewName).build();
    }

    public static ExportToGafferResultCache exportToGafferResult(final String key, final Set<String> opAuths) {
        return new ExportToGafferResultCache().key(key).opAuths(opAuths);
    }

    public static ExportToSet exportToSet(final String key) {
        return new ExportToSet().key(key);
    }

    public static Filter filterEdge(final String group, final ElementFilter filter) {
        return filterEdges(new SingletonMap(group, filter));
    }

    public static Filter filterEntity(final String group, final ElementFilter filter) {
        return filterEntities(new SingletonMap(group, filter));
    }

    public static Filter filterEdges(final Map<String, ElementFilter> edges) {
        return new Filter.Builder().edges(edges).build();
    }

    public static Filter filterEntities(final Map<String, ElementFilter> entities) {
        return new Filter.Builder().entities(entities).build();
    }

    public static Filter filterAllEdges(final ElementFilter filter) {
        return new Filter.Builder().globalEdges(filter).build();
    }

    public static Filter filterAllEntities(final ElementFilter filter) {
        return new Filter.Builder().globalEntities(filter).build();
    }

    public static Filter filterAllElements(final ElementFilter filter) {
        return new Filter.Builder().globalEdges(filter).build();
    }

    public static GetElements getEntities(final String... entities) {
        return get().view(createView().entities(entities));
    }

    public static GetElements getEdges(final String... edges) {
        return get().view(createView().edges(edges));
    }

    public static GetElements getElements() {
        return new GetElements();
    }

    public static GetAllElements getAllElements() {
        return new GetAllElements();
    }

    public static <I, O> If<I, O> condition() {
        return _if();
    }

    public static <I, O> If<I, O> _if() {
        return new If<>();
    }

    public static <I, O> While<I, O> repeat() {
        return _while();
    }

    public static While<Void, Void> repeat(final Operation op) {
        return _while(op);
    }

    public static <I> While<I, Void> repeat(final Input<I> op) {
        return _while(op);
    }

    public static <O> While<Void, O> repeat(final Output<O> op) {
        return _while(op);
    }

    public static <I, O> While<I, O> repeat(final InputOutput<I, O> op) {
        return _while(op);
    }

    public static <I, O> While<I, O> _while() {
        return new While<>();
    }

    public static While<Void, Void> _while(final Operation op) {
        return new While.Builder<Void, Void>().operation(op).build();
    }

    public static <I> While<I, Void> _while(final Input<I> op) {
        return new While.Builder<I, Void>().operation(op).build();
    }

    public static <O> While<Void, O> _while(final Output<O> op) {
        return new While.Builder<Void, O>().operation(op).build();
    }

    public static <I, O> While<I, O> _while(final InputOutput<I, O> op) {
        return new While.Builder<I, O>().operation(op).build();
    }

    public static <T> ToSet<T> deduplicate() {
        return toSet();
    }

    public static <T> ToSet<T> toSet() {
        return new ToSet<>();
    }

    public static Sort sort() {
        return new Sort();
    }

    public static ToCsv toCsv() {
        return new ToCsv();
    }

    public static class OpBuilder {
        private Object tmpInput;

        private static OpBuilder input(Object... input) {
            final OpBuilder opChain = new OpBuilder();
            if (1 == input.length) {
                opChain.tmpInput = input[0];
            }
            return opChain;
        }

        private static OpBuilder input(Iterable input) {
            final OpBuilder opChain = new OpBuilder();
            opChain.tmpInput = input;
            return opChain;
        }

        public OperationChain<Void> then(final Operation op) {
            setInput(op);
            return new OperationChain<>(op);
        }

        public <T> OperationChain<T> then(final Output<T> op) {
            setInput(op);
            return new OperationChain<>(op);
        }

        private void setInput(final Operation op) {
            if (null != tmpInput) {
                if (op instanceof Input) {
                    if (op instanceof MultiInput) {
                        if (op instanceof MultiEntityIdInput) {
                            setInput((MultiEntityIdInput) op);
                        } else if (op instanceof MultiElementIdInput) {
                            setInput((MultiElementIdInput) op);
                        } else {
                            setInput((MultiInput) op);
                        }
                    } else {
                        setInput(((Input) op));
                    }
                }
                tmpInput = null;
            }
        }

        private void setInput(final Input op) {
            if (null != tmpInput) {
                op.setInput(tmpInput);
                tmpInput = null;
            }
        }

        private void setInput(final MultiInput op) {
            if (null != tmpInput) {
                if (tmpInput instanceof Object[]) {
                    op.setInput(((Object[]) tmpInput));
                } else if (tmpInput instanceof Iterable) {
                    op.setInput((Iterable) tmpInput);
                } else {
                    op.setInput(new Object[]{tmpInput});
                }
                tmpInput = null;
            }
        }

        private void setInput(final MultiElementIdInput op) {
            if (null != tmpInput) {
                if (tmpInput instanceof Object[]) {
                    op.setInputFromVerticesAndIds(((Object[]) tmpInput));
                } else if (tmpInput instanceof Iterable) {
                    op.setInput(OperationUtil.toElementIds(tmpInput));
                } else {
                    op.setInputFromVerticesAndIds(new Object[]{tmpInput});
                }
                tmpInput = null;
            }
        }

        private void setInput(final MultiEntityIdInput op) {
            if (null != tmpInput) {
                if (tmpInput instanceof Object[]) {
                    op.setInputFromVerticesAndIds(((Object[]) tmpInput));
                } else if (tmpInput instanceof Iterable) {
                    op.setInput(OperationUtil.toEntityIds(tmpInput));
                } else {
                    op.setInputFromVerticesAndIds(new Object[]{tmpInput});
                }
                tmpInput = null;
            }
        }
    }
}
