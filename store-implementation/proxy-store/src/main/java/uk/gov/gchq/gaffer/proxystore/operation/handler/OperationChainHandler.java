/*
 * Copyright 2017-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.proxystore.operation.handler;

import com.google.common.collect.Lists;

import uk.gov.gchq.gaffer.core.exception.GafferWrappedErrorRuntimeException;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.proxystore.ProxyStore;
import uk.gov.gchq.gaffer.proxystore.exception.ProxyStoreException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.OperationChainValidator;
import uk.gov.gchq.gaffer.store.operation.handler.util.OperationHandlerUtil;
import uk.gov.gchq.gaffer.store.optimiser.OperationChainOptimiser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Objects.isNull;

public class OperationChainHandler<OUT> extends uk.gov.gchq.gaffer.store.operation.handler.OperationChainHandler<OUT> {

    public static final String PROXY_STORE_OPERATION_CHAIN_HANDLER = "proxyStore.operationChainHandler";
    public static final String TO_PROXY = "toProxy";
    public static final String TO_HANDLERS = "toHandlers";
    public static final String RESOLVED = "resolved";
    public static final String UNPROCESSED = "unprocessed";

    public OperationChainHandler(final OperationChainValidator opChainValidator, final List<OperationChainOptimiser> opChainOptimisers) {
        super(opChainValidator, opChainOptimisers);
    }

    @Override
    public OUT doOperation(final OperationChain<OUT> operationChain, final Context context, final Store store) {
        try {
            if (store instanceof ProxyStore) {
                switch (getProxyOptions(operationChain)) {
                    case RESOLVED:
                        return resolvedLogic(operationChain, context, store);

                    case TO_HANDLERS:
                        return handlersLogic(operationChain, context, store);

                    case TO_PROXY:
                        return proxyLogic(operationChain, context, (ProxyStore) store);

                    case UNPROCESSED:
                        /* Drop into default */

                    default:
                        return unprocessedLogic(operationChain, context, store);
                }
            } else {
                throw new ProxyStoreException("Store was not an instance of ProxyStore, found: " + store);
            }
        } catch (final ProxyStoreException | GafferWrappedErrorRuntimeException e) {
            throw e;
        } catch (final Exception e) {
            throw new ProxyStoreException("Error occurred processing " + this.getClass(), e);
        }
    }

    private String getProxyOptions(final OperationChain<OUT> operationChain) {
        final Map<String, String> options = operationChain.getOptions();
        return isNull(options) ? UNPROCESSED : options.getOrDefault(PROXY_STORE_OPERATION_CHAIN_HANDLER, UNPROCESSED);
    }

    private OUT unprocessedLogic(final OperationChain<OUT> operationChain, final Context context, final Store store) throws OperationException {
        List<Operation> operations = operationChain.getOperations();

        ArrayList<OperationChain> listOfOpChain = Lists.newArrayList();
        int placeMarker = 0;

        while (placeMarker < operations.size()) {
            ArrayList<Operation> unhandledOperations = Lists.newArrayList();
            ArrayList<Operation> handledOperations = Lists.newArrayList();

            for (int i = placeMarker; i < operations.size(); i++, placeMarker++) {
                Operation operation = operations.get(i);
                Class<? extends Operation> opClass = operation.getClass();
                if (isNull(store.getOperationHandler(opClass))) {
                    processAlternatingLists(listOfOpChain, unhandledOperations, handledOperations, operation, false);
                } else {
                    processAlternatingLists(listOfOpChain, handledOperations, unhandledOperations, operation, true);
                }
            }

            if (!unhandledOperations.isEmpty() && !handledOperations.isEmpty()) {
                throw new ProxyStoreException("No operations where processed by unprocessedLogic within:" + this.getClass().getName());
            }
            ifAlternativeOperationsExists(listOfOpChain, handledOperations, true);
            ifAlternativeOperationsExists(listOfOpChain, unhandledOperations, false);
        }

        OperationChain<OUT> rtn = opChainsToOpChain(listOfOpChain);
        rtn.setOptions(operationChain.getOptions());
        rtn.addOption(PROXY_STORE_OPERATION_CHAIN_HANDLER, RESOLVED);

        return this.doOperation(rtn, context, store);
    }

    private OUT proxyLogic(final OperationChain<OUT> operationChain, final Context context, final ProxyStore store) throws OperationException {
        return store.executeOpChainViaUrl(operationChain, context);
    }

    private OUT handlersLogic(final OperationChain<OUT> operationChain, final Context context, final Store store) throws OperationException {
        return super.doOperation(operationChain, context, store);
    }

    private OUT resolvedLogic(final OperationChain<OUT> operationChain, final Context context, final Store store) throws OperationException {
        Object out = null;
        for (final Operation operation : operationChain.getOperations()) {
            if (operation instanceof OperationChain) {
                //noinspection unchecked
                OperationChain chain = (OperationChain) operation;
                OperationHandlerUtil.updateOperationInput(chain, out);
                if (TO_PROXY.equals(chain.getOptions().get(PROXY_STORE_OPERATION_CHAIN_HANDLER))) {
                    out = proxyLogic(chain, context, (ProxyStore) store);
                } else {
                    //Generic is of type Object
                    out = new OperationChainHandler<>(getOpChainValidator(), getOpChainOptimisers()).doOperation(chain, context, store);
                }
            } else {
                throw new ProxyStoreException("While resolving opChains for resolvedLogic, expected OperationChain found: " + operation);
            }
        }

        OUT rtn;
        try {
            //noinspection unchecked
            rtn = (OUT) out;
        } catch (final ClassCastException e) {
            throw new ProxyStoreException("The return type was not of expected type", e);
        }
        return rtn;
    }

    private void processAlternatingLists(final ArrayList<OperationChain> opChainOfOpChain, final ArrayList<Operation> currentOperations, final ArrayList<Operation> altOperations, final Operation operation, final boolean hasOperationHandler) {
        currentOperations.add(operation);
        ifAlternativeOperationsExists(opChainOfOpChain, altOperations, !hasOperationHandler);
    }

    private void ifAlternativeOperationsExists(final ArrayList<OperationChain> opChainOfOpChain, final ArrayList<Operation> operations, final boolean hasOperationHandler) {
        if (!operations.isEmpty()) {
            //Append alt operations to a opChain
            OperationChain opchain = operationsToOpChain(operations);

            opchain.addOption(PROXY_STORE_OPERATION_CHAIN_HANDLER, hasOperationHandler ? TO_HANDLERS : TO_PROXY);
            //Add op chain
            opChainOfOpChain.add(opchain);
        }
    }

    private OperationChain<OUT> opChainsToOpChain(final ArrayList<OperationChain> list) {
        OperationChain<OUT> rtn = new OperationChain(list);

        //clear alt list
        list.clear();
        return rtn;
    }

    private OperationChain operationsToOpChain(final ArrayList<Operation> list) {
        OperationChain<Object> rtn = new OperationChain<>();
        rtn.updateOperations(list);

        //clear alt list
        list.clear();
        return rtn;
    }

}
