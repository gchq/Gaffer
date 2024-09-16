/*
 * Copyright 2016-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.export.resultcache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.AlwaysValid;
import uk.gov.gchq.gaffer.commonutil.iterable.EmptyIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.TransformIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.export.Exporter;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.koryphe.impl.predicate.AreIn;
import uk.gov.gchq.koryphe.serialisation.json.SimpleClassNameIdResolver;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import static java.util.Objects.isNull;

/**
 * Implementation of the {@link Exporter} interface for exporting the results of
 * a Gaffer query to a {@link Graph}-backed results cache.
 */
public class GafferResultCacheExporter implements Exporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(GafferResultCacheExporter.class);
    private final String jobId;
    private final Context context;
    private final Graph resultCache;
    private final String visibility;
    private final TreeSet<String> requiredOpAuths;
    private final Set<String> userOpAuths;

    public GafferResultCacheExporter(final Context context,
                                     final String jobId,
                                     final Graph resultCache,
                                     final String visibility,
                                     final Set<String> requiredOpAuths) {
        this.context = context;
        this.jobId = jobId;
        this.resultCache = resultCache;
        this.visibility = visibility;
        if (null == requiredOpAuths) {
            this.requiredOpAuths = CollectionUtil.treeSet(context.getUser().getUserId());
        } else {
            this.requiredOpAuths = new TreeSet<>(requiredOpAuths);
        }

        userOpAuths = new HashSet<>(context.getUser().getOpAuths());
        userOpAuths.add(context.getUser().getUserId());
    }

    @Override
    public void add(final String key, final Iterable<?> values) throws OperationException {
        if (isNull(values)) {
            return;
        }

        final long timestamp = System.currentTimeMillis();
        final Iterable<Element> elements = new TransformIterable<Object, Element>(values) {
            @Override
            protected Element transform(final Object value) {
                try {
                    final Class<?> valueClass;
                    final byte[] valueJson;
                    if (isNull(value)) {
                        valueClass = Object.class;
                        valueJson = null;
                    } else {
                        valueClass = value.getClass();
                        valueJson = JSONSerialiser.serialise(value);
                    }

                    return new Edge.Builder()
                            .group("result")
                            .source(jobId)
                            .dest(key)
                            .directed(true)
                            .property("opAuths", requiredOpAuths)
                            .property("timestamp", timestamp)
                            .property("visibility", visibility)
                            .property("resultClass", valueClass.getName())
                            .property("result", valueJson)
                            .build();
                } catch (final SerialisationException e) {
                    throw new RuntimeException("Unable to serialise results to json", e);
                }
            }
        };

        resultCache.execute(new AddElements.Builder()
                .input(elements)
                .build(), context);
    }

    @Override
    public Iterable<?> get(final String key) throws OperationException {
        final GetElements getEdges = new GetElements.Builder()
                .input(new EdgeSeed(jobId, key, true))
                .view(new View.Builder()
                        .edge("result", new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select("opAuths")
                                        .execute(new AreIn(userOpAuths))
                                        .build())
                                .build())
                        .build())
                .build();

        final Iterable<? extends Element> edges = resultCache.execute(getEdges, context);
        if (isNull(edges)) {
            return new EmptyIterable<>();
        }
        return new TransformJsonResult(edges);
    }

    private static class TransformJsonResult extends TransformIterable<Element, Object> {
        TransformJsonResult(final Iterable<? extends Element> input) {
            super(input, new AlwaysValid<>(), false, true);
        }

        @Override
        protected Object transform(final Element edge) {
            final String resultClassName = (String) edge.getProperty("resultClass");
            final byte[] resultBytes = (byte[]) edge.getProperty("result");
            if (isNull(resultClassName) || isNull(resultBytes)) {
                return null;
            }

            final Class<?> resultClass;
            try {
                resultClass = Class.forName(SimpleClassNameIdResolver.getClassName(resultClassName));
            } catch (final ClassNotFoundException e) {
                LOGGER.error("Result class name was not found: {}", resultClassName, e);
                throw new RuntimeException(e);
            }

            try {
                return JSONSerialiser.deserialise(resultBytes, resultClass);
            } catch (final SerialisationException e) {
                LOGGER.error("Unable to deserialise result: {}", new String(resultBytes, StandardCharsets.UTF_8), e);
                throw new RuntimeException(e);
            }
        }
    }

    protected String getJobId() {
        return jobId;
    }

    protected Context getContext() {
        return context;
    }

    protected Graph getResultCache() {
        return resultCache;
    }

    protected String getVisibility() {
        return visibility;
    }

    protected TreeSet<String> getRequiredOpAuths() {
        return requiredOpAuths;
    }

    protected Set<String> getUserOpAuths() {
        return userOpAuths;
    }
}
