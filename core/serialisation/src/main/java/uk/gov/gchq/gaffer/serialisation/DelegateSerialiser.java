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

package uk.gov.gchq.gaffer.serialisation;

import uk.gov.gchq.gaffer.exception.SerialisationException;

/**
 * Abstract serialiser for serialising types to bytes by delegating to a different
 * serialiser.
 *
 * @param <T> type to delegate from
 * @param <U> type to delegate to
 */
public abstract class DelegateSerialiser<T, U> implements ToBytesSerialiser<T> {

    public final ToBytesSerialiser<U> delegateSerialiser;

    public DelegateSerialiser(final ToBytesSerialiser<U> delegateSerialiser) {
        this.delegateSerialiser = delegateSerialiser;
    }

    @Override
    public byte[] serialise(final T object) throws SerialisationException {
        return delegateSerialiser.serialise(toDelegateType(object));
    }

    @Override
    public T deserialise(final byte[] bytes) throws SerialisationException {
        return fromDelegateType(delegateSerialiser.deserialise(bytes));
    }

    @Override
    public T deserialiseEmpty() throws SerialisationException {
        final U empty = delegateSerialiser.deserialiseEmpty();
        if (null != empty) {
            return fromDelegateType(empty);
        }
        return null;
    }

    @Override
    public boolean preservesObjectOrdering() {
        return delegateSerialiser.preservesObjectOrdering();
    }

    public abstract T fromDelegateType(final U object);

    public abstract U toDelegateType(final T object);

}
