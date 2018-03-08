/*
 * Copyright 2017-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.graph.hook;

import org.apache.commons.collections.CollectionUtils;

import uk.gov.gchq.gaffer.data.elementdefinition.view.NamedView;
import uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.named.operation.cache.exception.CacheOperationFailedException;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.Operations;
import uk.gov.gchq.gaffer.operation.graph.OperationView;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.operation.handler.named.cache.NamedViewCache;

import java.util.Map;

/**
 * A {@link GraphHook} to resolve {@link NamedView}s.
 */
public class NamedViewResolver implements GraphHook {
    private final NamedViewCache cache;

    public NamedViewResolver() {
        cache = new NamedViewCache();
    }

    public NamedViewResolver(final NamedViewCache cache) {
        this.cache = cache;
    }

    @Override
    public void preExecute(final OperationChain<?> opChain, final Context context) {
        resolveViews(opChain);
    }

    @Override
    public <T> T postExecute(final T result, final OperationChain<?> opChain, final Context context) {
        return result;
    }

    @Override
    public <T> T onFailure(final T result, final OperationChain<?> opChain, final Context context, final Exception e) {
        return result;
    }

    private void resolveViews(final Operations<?> operations) {
        for (final Operation operation : operations.getOperations()) {
            if (operation instanceof OperationView) {
                final OperationView opView = ((OperationView) operation);
                if (opView.getView() instanceof NamedView) {
                    opView.setView(resolveView((NamedView) opView.getView()));
                }
            } else if (operation instanceof Operations) {
                resolveViews((Operations<?>) operation);
            }
        }
    }

    private View resolveView(final NamedView namedView) {
        View resolvedView = resolveView(namedView.getName(), namedView.getParameters());
        if (CollectionUtils.isNotEmpty(namedView.getMergedNamedViewNames())) {
            final View.Builder viewBuilder = new View.Builder();
            viewBuilder.merge(resolvedView);
            for (final String name : namedView.getMergedNamedViewNames()) {
                viewBuilder.merge(resolveView(name, namedView.getParameters()));
            }
            resolvedView = viewBuilder.build();
        }

        namedView.setName(null);
        return new View.Builder()
                .merge(resolvedView)
                .merge(namedView)
                .build();
    }

    private View resolveView(final String namedViewName, final Map<String, Object> parameters) {
        final NamedViewDetail cachedNamedView;
        try {
            cachedNamedView = cache.getNamedView(namedViewName);
        } catch (final CacheOperationFailedException e) {
            throw new RuntimeException(e);
        }

        View resolvedView;
        if (null == cachedNamedView) {
            resolvedView = new View();
        } else {
            resolvedView = cachedNamedView.getView(parameters);
            if (resolvedView instanceof NamedView) {
                ((NamedView) resolvedView).setName(null);
                if (CollectionUtils.isNotEmpty(((NamedView) resolvedView).getMergedNamedViewNames())) {
                    final View.Builder viewBuilder = new View.Builder();
                    viewBuilder.merge(resolvedView);
                    for (final String name : ((NamedView) resolvedView).getMergedNamedViewNames()) {
                        viewBuilder.merge(resolveView(name, parameters));
                    }
                    resolvedView = viewBuilder.build();
                }
            }
        }

        return resolvedView;
    }
}
