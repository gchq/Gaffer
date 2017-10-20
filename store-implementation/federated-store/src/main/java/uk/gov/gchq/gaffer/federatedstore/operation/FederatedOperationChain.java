/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.operation;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;


public class FederatedOperationChain<O_ITEM> extends OperationChain<CloseableIterable<O_ITEM>> {
    public FederatedOperationChain(final Operation operation) {
        this(OperationChain.wrap(operation));
    }

    public FederatedOperationChain(final OperationChain<?> operationChain) {
        super(operationChain.getOperations());
        setOptions(operationChain.getOptions());
    }

    @Override
    public TypeReference<CloseableIterable<O_ITEM>> getOutputTypeReference() {
        return (TypeReference) new TypeReferenceImpl.CloseableIterableObj();
    }

    @JsonIgnore
    public TypeReference<CloseableIterable<O_ITEM>> getOperationChainOutputTypeReference() {
        return super.getOutputTypeReference();
    }

    @JsonIgnore
    public Class<?> getOperationChainOutputClass() {
        Class<?> outputClass = Object.class;

        final TypeReference outputType = getOperationChainOutputTypeReference();
        if (null != outputType) {
            Type type = outputType.getType();
            if (type instanceof ParameterizedType) {
                type = ((ParameterizedType) type).getRawType();
            }
            if (type instanceof Class) {
                outputClass = (Class) type;
            }
        }

        return outputClass;
    }

    @Override
    public FederatedOperationChain<O_ITEM> shallowClone() throws CloneFailedException {
        return new FederatedOperationChain<>(super.shallowClone());
    }

    @JsonIgnore
    public OperationChain<?> getOperationChain() {
        final OperationChain<?> opChain = new OperationChain<>(getOperations());
        opChain.setOptions(getOptions());
        return opChain;
    }
}
