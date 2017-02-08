/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.named.operation.handler;


import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.named.operation.cache.INamedOperationCache;
import uk.gov.gchq.gaffer.named.operation.cache.MockNamedOperationCache;
import uk.gov.gchq.gaffer.store.operationdeclaration.OperationDeclarations;

import java.io.InputStream;

import static org.junit.Assert.assertEquals;


public class NamedOperationHandlerTest {
    private final JSONSerialiser json = new JSONSerialiser();

    @Test
    public void shouldLoadFromNamedOperationDeclarationsFile() throws SerialisationException {
        final InputStream s = StreamUtil.openStream(getClass(), "NamedOperationDeclarations.json");
        final OperationDeclarations deserialised = json.deserialise(s, OperationDeclarations.class);

        assertEquals(4, deserialised.getOperations().size());
        assert(deserialised.getOperations().get(0).getHandler() instanceof AddNamedOperationHandler);
        assert(deserialised.getOperations().get(1).getHandler() instanceof NamedOperationHandler);
        assert(deserialised.getOperations().get(2).getHandler() instanceof DeleteNamedOperationHandler);
        assert(deserialised.getOperations().get(3).getHandler() instanceof GetAllNamedOperationsHandler);
    }

    @Test
    public void shouldLoadCacheFromOperationsDeclarationsFile() throws SerialisationException {
        final InputStream s = StreamUtil.openStream(getClass(), "NamedOperationDeclarations.json");
        final OperationDeclarations deserialised = json.deserialise(s, OperationDeclarations.class);

        INamedOperationCache cache = ((AddNamedOperationHandler) deserialised.getOperations().get(0).getHandler()).getCache();

        assert(cache instanceof MockNamedOperationCache);
    }

}
