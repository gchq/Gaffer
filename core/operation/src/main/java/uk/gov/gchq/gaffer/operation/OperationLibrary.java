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

package uk.gov.gchq.gaffer.operation;

import com.google.common.collect.Lists;
import org.apache.commons.collections.map.SingletonMap;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.named.operation.AddNamedOperation;
import uk.gov.gchq.gaffer.named.view.AddNamedView;
import uk.gov.gchq.gaffer.named.view.DeleteNamedView;
import uk.gov.gchq.gaffer.named.view.GetAllNamedViews;
import uk.gov.gchq.gaffer.operation.impl.Count;
import uk.gov.gchq.gaffer.operation.impl.CountGroups;
import uk.gov.gchq.gaffer.operation.impl.DiscardOutput;
import uk.gov.gchq.gaffer.operation.impl.ForEach;
import uk.gov.gchq.gaffer.operation.impl.GetVariable;
import uk.gov.gchq.gaffer.operation.impl.GetVariables;
import uk.gov.gchq.gaffer.operation.impl.GetWalks;
import uk.gov.gchq.gaffer.operation.impl.If;
import uk.gov.gchq.gaffer.operation.impl.Limit;
import uk.gov.gchq.gaffer.operation.impl.Reduce;
import uk.gov.gchq.gaffer.operation.impl.SampleElementsForSplitPoints;
import uk.gov.gchq.gaffer.operation.impl.ScoreOperationChain;
import uk.gov.gchq.gaffer.operation.impl.SetVariable;
import uk.gov.gchq.gaffer.operation.impl.SplitStoreFromFile;
import uk.gov.gchq.gaffer.operation.impl.SplitStoreFromIterable;
import uk.gov.gchq.gaffer.operation.impl.Validate;
import uk.gov.gchq.gaffer.operation.impl.ValidateOperationChain;
import uk.gov.gchq.gaffer.operation.impl.While;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.add.AddElementsFromFile;
import uk.gov.gchq.gaffer.operation.impl.add.AddElementsFromKafka;
import uk.gov.gchq.gaffer.operation.impl.add.AddElementsFromSocket;
import uk.gov.gchq.gaffer.operation.impl.compare.Max;
import uk.gov.gchq.gaffer.operation.impl.compare.Min;
import uk.gov.gchq.gaffer.operation.impl.compare.Sort;
import uk.gov.gchq.gaffer.operation.impl.export.GetExports;
import uk.gov.gchq.gaffer.operation.impl.export.resultcache.ExportToGafferResultCache;
import uk.gov.gchq.gaffer.operation.impl.export.resultcache.GetGafferResultCacheExport;
import uk.gov.gchq.gaffer.operation.impl.export.set.ExportToSet;
import uk.gov.gchq.gaffer.operation.impl.export.set.GetSetExport;
import uk.gov.gchq.gaffer.operation.impl.function.Aggregate;
import uk.gov.gchq.gaffer.operation.impl.function.Filter;
import uk.gov.gchq.gaffer.operation.impl.function.Transform;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.impl.job.GetAllJobDetails;
import uk.gov.gchq.gaffer.operation.impl.job.GetJobDetails;
import uk.gov.gchq.gaffer.operation.impl.job.GetJobResults;
import uk.gov.gchq.gaffer.operation.impl.output.ToArray;
import uk.gov.gchq.gaffer.operation.impl.output.ToCsv;
import uk.gov.gchq.gaffer.operation.impl.output.ToEntitySeeds;
import uk.gov.gchq.gaffer.operation.impl.output.ToList;
import uk.gov.gchq.gaffer.operation.impl.output.ToMap;
import uk.gov.gchq.gaffer.operation.impl.output.ToSet;
import uk.gov.gchq.gaffer.operation.impl.output.ToSingletonList;
import uk.gov.gchq.gaffer.operation.impl.output.ToStream;
import uk.gov.gchq.gaffer.operation.impl.output.ToVertices;
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiElementIdInput;
import uk.gov.gchq.gaffer.operation.io.MultiEntityIdInput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.operation.util.AggregatePair;
import uk.gov.gchq.gaffer.operation.util.OperationUtil;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Function;

import static uk.gov.gchq.gaffer.data.elementdefinition.view.View.createView;

public class OperationLibrary {
    public static OperationChain<Void> chain() {
        return new OperationChain<>();
    }

    public static OpBuilder input(final Object... input) {
        return OpBuilder.input(input);
    }

    public static <T> ToSet<T> deduplicate() {
        return toSet();
    }

    public static <I, O> If<I, O> condition() {
        return _if();
    }
    
    public static AddNamedOperation addNamedOperation(final String operationName, final OperationChain operations) {
        return new AddNamedOperation.Builder().name(operationName).operationChain(operations).build();
    }

    public static AddNamedView addNamedView(final String name, final View view, final List<String> writeAccessRoles) {
        return new AddNamedView.Builder().name(name).view(view).writeAccessRoles(writeAccessRoles).build();
    }

    public static AddNamedView addNamedView(final String name, final View view, final String... writeAccessRoles) {
        return new AddNamedView.Builder().name(name).view(view).writeAccessRoles(Lists.newArrayList(writeAccessRoles)).build();
    }

    public static DeleteNamedView deleteNamedView(final String viewName) {
        return new DeleteNamedView.Builder().name(viewName).build();
    }

    public static GetAllNamedViews getAllNamedViews() {
        return new GetAllNamedViews();
    }

    public static AddElements addElements() {
        return new AddElements();
    }

    public static AddElementsFromFile addElementsFromFile(final String fileName, final Class<? extends Function<Iterable<? extends String>, Iterable<? extends Element>>> elementGenerator) {
        return new AddElementsFromFile.Builder().filename(fileName).generator(elementGenerator).build();
    }

    public static AddElementsFromKafka addElementsFromKafka(final String topic, final String groupId, final String[] bootstrapServers, final Class<? extends Function<Iterable<? extends String>, Iterable<? extends Element>>> elementGenerator) {
        return new AddElementsFromKafka.Builder().topic(topic).groupId(groupId).bootstrapServers(bootstrapServers).generator(elementGenerator).build();
    }

    public static AddElementsFromSocket addElementsFromSocket(final String hostname, final int port, final Class<? extends Function<Iterable<? extends String>, Iterable<? extends Element>>> elementGenerator) {
        return new AddElementsFromSocket.Builder().hostname(hostname).port(port).generator(elementGenerator).build();
    }

    public static Max max(final List<Comparator<Element>> comparators) {
        return new Max.Builder().comparators(comparators).build();
    }

    public static Min min(final List<Comparator<Element>> comparators) {
        return new Min.Builder().comparators(comparators).build();
    }

    public static Sort sort(final List<Comparator<Element>> comparators) {
        return new Sort.Builder().comparators(comparators).build();
    }

    public static Sort sort() {
        return new Sort();
    }

    public static ExportToGafferResultCache exportToGafferResult(final String key, final Set<String> opAuths) {
        return new ExportToGafferResultCache().key(key).opAuths(opAuths);
    }

    public static GetGafferResultCacheExport getGafferResult(final String key) {
        return new GetGafferResultCacheExport.Builder().key(key).build();
    }

    public static GetGafferResultCacheExport getGafferResult(final String key, final String jobId) {
        return new GetGafferResultCacheExport.Builder().key(key).jobId(jobId).build();
    }

    public static ExportToSet exportToSet(final String key) {
        return new ExportToSet.Builder<>().key(key).build();
    }

    public static GetSetExport getExportedSet(final String key) {
        return new GetSetExport.Builder().key(key).build();
    }

    public static GetExports getExports() {
        return new GetExports();
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

    public static Transform transformEdges(final Map<String, ElementTransformer> edges) {
        return new Transform.Builder().edges(edges).build();
    }

    public static Transform transformEntites(final Map<String, ElementTransformer> entities) {
        return new Transform.Builder().entities(entities).build();
    }

    public static GetAdjacentIds hop(final String... edges) {
        return getAdjacentIds(edges);
    }

    public static GetAdjacentIds getAdjacentIds(final String... edges) {
        return new GetAdjacentIds().view(createView().edges(edges));
    }

    public static GetAllElements getAll() {
        return getAllElements();
    }

    public static GetAllElements getAllElements() {
        return new GetAllElements();
    }

    public static GetElements getEntities(final String... entities) {
        return get().view(createView().entities(entities));
    }

    public static GetElements getEdges(final String... edges) {
        return get().view(createView().edges(edges));
    }

    public static GetElements get() {
        return getElements();
    }

    public static GetElements getElements() {
        return new GetElements();
    }

    public static GetAllJobDetails getAllJobDetails() {
        return new GetAllJobDetails();
    }

    public static GetJobDetails getJobDetails(final String jobId) {
        return new GetJobDetails.Builder().jobId(jobId).build();
    }

    public static GetJobResults getJobResults(final String jobId) {
        return new GetJobResults.Builder().jobId(jobId).build();
    }

    public static <T> ToArray<T> toArray() {
        return new ToArray();
    }

    public static ToCsv toCsv() {
        return new ToCsv();
    }

    public static ToEntitySeeds toEntitySeeds() {
        return new ToEntitySeeds();
    }

    public static <T> ToList<T> toList() {
        return new ToList<>();
    }

    public static ToMap toMap() {
        return new ToMap();
    }

    public static <T> ToSet<T> toSet() {
        return new ToSet<>();
    }

    public static <T> ToSingletonList<T> toSingletonList() {
        return new ToSingletonList<>();
    }

    public static <T> ToStream<T> toStream() {
        return new ToStream<>();
    }

    public static ToVertices toVertices() {
        return new ToVertices();
    }

    public static <T> Count<T> count() {
        return new Count<>();
    }

    public static CountGroups countGroups() {
        return new CountGroups();
    }

    public static DiscardOutput discardOutput() {
        return new DiscardOutput();
    }

    public static <I, O> ForEach<I, O> forEach() {
        return new ForEach<>();
    }

    public static ForEach<Void, Void> forEach(final Operation op) {
        return new ForEach.Builder<Void, Void>().operation(op).build();
    }

    public static <I> ForEach<I, Void> forEach(final Input<I> op) {
        return new ForEach.Builder<I, Void>().operation(op).build();
    }

    public static <O> ForEach<Void, O> forEach(final Output<O> op) {
        return new ForEach.Builder<Void, O>().operation(op).build();
    }

    public static <I, O> ForEach<I, O> forEach(final InputOutput<I, O> op) {
        return new ForEach.Builder<I, O>().operation(op).build();
    }

    public static GetVariable getVariable(final String variableName) {
        return new GetVariable.Builder().variableName(variableName).build();
    }

    public static GetVariables getVariables(final List<String> variableNames) {
        return new GetVariables.Builder().variableNames(variableNames).build();
    }

    public static GetWalks walk() {
        return getWalks();
    }

    public static GetWalks getWalks() {
        return new GetWalks();
    }

    public static <I, O> If<I, O> _if() {
        return new If<>();
    }

    public static <T> Limit<T> limit(final Integer limit) {
        return new Limit.Builder<T>().resultLimit(limit).build();
    }

    public static <I> uk.gov.gchq.gaffer.operation.impl.Map<I, Object> map(final I input) {
        return new uk.gov.gchq.gaffer.operation.impl.Map.Builder<I>().input(input).build();
    }

    public static <T> Reduce<T> reduce(final BinaryOperator aggregateFunction) {
        return new Reduce.Builder<T>().aggregateFunction(aggregateFunction).build();
    }

    public static <T> Reduce<T> reduce(final BinaryOperator aggregateFunction, final T identity) {
        return new Reduce.Builder<T>().aggregateFunction(aggregateFunction).identity(identity).build();
    }

    public static <T> SampleElementsForSplitPoints<T> sampleDataForSplitPoints() {
        return new SampleElementsForSplitPoints<>();
    }

    public static ScoreOperationChain scoreOperationChain(final OperationChain operationChain) {
        return new ScoreOperationChain.Builder().operationChain(operationChain).build();
    }

    public static SetVariable setVariable(final String variableName) {
        return new SetVariable.Builder().variableName(variableName).build();
    }

    public static SplitStoreFromFile splitStoreFromFile(final String inputPath) {
        return new SplitStoreFromFile.Builder().inputPath(inputPath).build();
    }

    public static <T> SplitStoreFromIterable<T> splitStoreFromIterable() {
        return new SplitStoreFromIterable<>();
    }

    public static Validate validate() {
        return new Validate();
    }

    public static ValidateOperationChain validateOperationChain(final OperationChain operationChain) {
        return new ValidateOperationChain.Builder().operationChain(operationChain).build();
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
