/*
 * Copyright 2022 Crown Copyright
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
package uk.gov.gchq.gaffer.federatedstore.util;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.core.exception.GafferCheckedException;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.function.BiFunction;

import static java.util.Objects.nonNull;
import static uk.gov.gchq.gaffer.mapstore.impl.GetElementsUtil.applyView;

public class ApplyViewToElementsFunction implements BiFunction<Object, Iterable<Object>, Iterable<Object>>, ContextSpecificMergeFunction<Object, Iterable<Object>, Iterable<Object>> {

    public static final String MISSING_S = "Context is not complete for %s missing: %s";
    public static final String VIEW = "view";
    public static final String SCHEMA = "schema";
    public static final String FEDERATED_STORE = "federatedStore";
    public static final String CONTEXT = "context";
    public static final String TEMP_GRAPH_NAME = "tempGraphName";
    HashMap<String, Object> context;

    public ApplyViewToElementsFunction() {
    }

    public ApplyViewToElementsFunction(final HashMap<String, Object> context) throws GafferCheckedException {
        this();
        this.context = validate(context);
        final FederatedStore federatedStore = (FederatedStore) context.get(FEDERATED_STORE);

        final String graphId = "temp" + new Random().nextInt(1000);
        context.put(TEMP_GRAPH_NAME, graphId);
        try {
            federatedStore.execute(new AddGraph.Builder().graphId(graphId).schema((Schema) context.get(SCHEMA)).storeProperties(new MapStoreProperties()).build(), (Context) context.get(CONTEXT));
        } catch (final Exception e) {
            throw new GafferCheckedException("Unable to create temporary MapStore of results",e);
        }
    }

    @Override
    public ApplyViewToElementsFunction createFunctionWithContext(final HashMap<String, Object> context) throws GafferCheckedException {
        return new ApplyViewToElementsFunction(context);
    }

    private static HashMap<String, Object> validate(final HashMap<String, Object> context) {

        final FederatedStore federatedStore = (FederatedStore) context.get(FEDERATED_STORE);
        if (federatedStore == null) {
            throw new UnsupportedOperationException("This function needs a FederatedS tore to produce a temporary MapStore for results");
        }
        final Context storeContext = (Context) context.get(CONTEXT);
        if (storeContext == null) {
            throw new UnsupportedOperationException("This function needs a context to produce a temporary MapStore for results");
        }
        View view = (View) context.get(VIEW);
        if (view == null) {
            context.put(VIEW, new View()); //TODO FS CAN/CAN'T HAVE EMPTY VIEW!?!?!?!
            // throw new IllegalArgumentException(String.format(MISSING_S, ApplyViewToElementsFunction.class.getCanonicalName(), VIEW));
        } else if (view.hasTransform()) {
            throw new UnsupportedOperationException("Error: can not use the default merge function with a POST AGGREGATION TRANSFORM VIEW, " +
                    "because transformation may have created items that does not exist in the schema. " +
                    "The re-applying of the View to the collected federated results would not be be possible. " +
                    "Try a simple concat merge that doesn't require the re-application of view");
            //Solution is to derive and use the "Transformed schema" from the uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition.
        }
        Schema schema = (Schema) context.get(SCHEMA);
        if (schema == null) {
            context.put(SCHEMA, new Schema());
            // throw new IllegalArgumentException(String.format(MISSING_S, ApplyViewToElementsFunction.class.getCanonicalName(), SCHEMA));
        }
        return context;
    }

    @Override
    public Set<String> getRequiredContextValues() {
        //TODO FS get Required/optional ContextValues too similar to Maestro
        return ImmutableSet.copyOf(new String[]{VIEW, SCHEMA, FEDERATED_STORE, CONTEXT});
    }

    @Override
    public Iterable<Object> apply(final Object first, final Iterable<Object> next) {
        //TODO FS Test
        if (next instanceof Closeable) {
            Closeable closeable = (Closeable) next;
            try {
                closeable.close();
            } catch (final IOException e) {
                throw new GafferRuntimeException("Error closing looped iterable", e);
            }
        }

        final FederatedStore federatedStore = (FederatedStore) context.get(FEDERATED_STORE);
        try {
            //TODO FS attention to Input
            federatedStore.execute(new AddElements.Builder().input((Iterable<Element>) first).build(), (Context) context.get(CONTEXT));
        } catch (final OperationException e) {
            throw new GafferRuntimeException("Error adding elements to temporary graph", e);
        }

        try {
            //TODO FS improve this horrible graph manipulation.
            final Collection<Graph> graphs = federatedStore.getGraphs(((Context) context.get(CONTEXT)).getUser(), (String) context.get(TEMP_GRAPH_NAME), new FederatedOperation<>());
            final Graph next1 = graphs.iterator().next();
            //TODO FS attention to VIEW
            return  (Iterable)    next1.execute(new GetAllElements.Builder().view((View) context.get(VIEW)).build(), (Context) context.get(CONTEXT));
        } catch (OperationException e) {
            throw new GafferRuntimeException("Error getting all elements from temporary graph", e);
        }
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(13, 47)
                .append(super.hashCode())
                .toHashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        return nonNull(obj) && this.getClass().equals(obj.getClass());
    }

    private static class ViewFilteredIterable implements Iterable<Object> {
        Iterable<Object> concatResults;
        View view;
        Schema schema;

        ViewFilteredIterable(final HashMap context) {
            this.view = (View) context.get(VIEW);
            this.schema = (Schema) context.get(SCHEMA);
            this.concatResults = (Iterable<Object>) context.get("concatResults");
            if (view == null || schema == null || concatResults == null) {
                throw new IllegalArgumentException(String.format(MISSING_S, ApplyViewToElementsFunction.class.getCanonicalName(), view == null ? VIEW : schema == null ? SCHEMA : "concatResults"));
            }
        }

        @Override
        public Iterator<Object> iterator() {
            //TODO FS test.
            return applyView(Streams.toStream(concatResults).map(o -> (Element) o), schema, view).map(o -> (Object) o).iterator();
        }
    }
}
