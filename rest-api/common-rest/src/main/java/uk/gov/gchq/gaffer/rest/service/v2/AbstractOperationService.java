/*
 * Copyright 2020-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.service.v2;

import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.core.exception.Status;
import uk.gov.gchq.gaffer.graph.GraphRequest;
import uk.gov.gchq.gaffer.graph.GraphResult;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.rest.factory.ExamplesFactory;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.factory.UserFactory;
import uk.gov.gchq.gaffer.rest.model.OperationDetail;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.koryphe.serialisation.json.SimpleClassNameIdResolver;
import uk.gov.gchq.koryphe.util.ReflectionUtil;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * An abstract OperationsService which allows for implementations to inject dependencies
 * depending on what DI framework they prefer. This abstraction allows Spring and Jersey
 * implementations share the same code
 */
@SuppressWarnings("PMD.EmptyMethodInAbstractClassShouldBeAbstract") //Class is not particularly abstract
public abstract class AbstractOperationService {

    protected abstract UserFactory getUserFactory();

    protected abstract ExamplesFactory getExamplesFactory();

    protected abstract GraphFactory getGraphFactory();

    public Set<Class <? extends Operation>> getSupportedOperations() {
        return getGraphFactory().getGraph().getSupportedOperations();
    }

    public Set<OperationDetail> getSupportedOperationDetails() {
        return getSupportedOperationDetails(false);
    }

    public Set<OperationDetail> getSupportedOperationDetails(final boolean includeUnsupported) {
        Set<Class<? extends Operation>> operationClasses;
        if (includeUnsupported) {
            operationClasses = new HashSet(ReflectionUtil.getSubTypes(Operation.class));
        } else {
            operationClasses = getSupportedOperations();
        }
        Set<OperationDetail> operationDetails = new TreeSet<>((operationDetail1, operationDetail2) -> {
            try {
                String simpleName1 = Class.forName(operationDetail1.getName()).asSubclass(Operation.class).getSimpleName();
                String simpleName2 = Class.forName(operationDetail2.getName()).asSubclass(Operation.class).getSimpleName();
                return simpleName1.compareTo(simpleName2);
            } catch (final ClassNotFoundException e) {
                throw new GafferRuntimeException("Class could not be found: ", e, Status.INTERNAL_SERVER_ERROR);
            }
        });
        for (final Class<? extends Operation> clazz : operationClasses) {
            try {
                operationDetails.add(new OperationDetail(clazz, getNextOperations(clazz), generateExampleJson(clazz)));
            } catch (final IllegalAccessException | InstantiationException e) {
                throw new GafferRuntimeException("Could not get operation details for class: " + clazz, e, Status.BAD_REQUEST);
            }
        }

        return operationDetails;
    }

    protected void preOperationHook(final OperationChain<?> opChain, final Context context) {
        // no action by default
    }

    protected void postOperationHook(final OperationChain<?> opChain, final Context context) {
        // no action by default
    }

    @SuppressWarnings({"ThrowFromFinallyBlock", "PMD.UseTryWithResources"})
    protected <O> Pair<O, String> _execute(final Operation operation, final Context context) {

        OperationChain<O> opChain = (OperationChain<O>) OperationChain.wrap(operation);

        preOperationHook(opChain, context);

        GraphResult<O> result;
        try {
            result = getGraphFactory().getGraph().execute(new GraphRequest<>(opChain, context));
        } catch (final OperationException e) {
            CloseableUtil.close(operation);
            final String message = null != e.getMessage() ? "Error executing opChain: " + e.getMessage() : "Error executing opChain";
            throw new GafferRuntimeException(message, e, e.getStatus());
        } finally {
            try {
                postOperationHook(opChain, context);
            } catch (final Exception e) {
                CloseableUtil.close(operation);
                throw e;
            }
        }

        return new Pair<>(result.getResult(), result.getContext().getJobId());
    }

    protected Operation generateExampleJson(final Class<? extends Operation> opClass) throws IllegalAccessException, InstantiationException {
        return getExamplesFactory().generateExample(opClass);
    }

    protected Set<Class<? extends Operation>> getNextOperations(final Class<? extends Operation> opClass) {
        return getGraphFactory().getGraph().getNextOperations(opClass);
    }

    protected Class<? extends Operation> getOperationClass(final String className) throws ClassNotFoundException {
        return Class.forName(SimpleClassNameIdResolver.getClassName(className)).asSubclass(Operation.class);
    }

}
