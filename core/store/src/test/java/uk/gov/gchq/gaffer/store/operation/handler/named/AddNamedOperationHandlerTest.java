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

package uk.gov.gchq.gaffer.store.operation.handler.named;

import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.named.operation.AddNamedOperation;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.named.operation.NamedOperationDetail;
import uk.gov.gchq.gaffer.named.operation.ParameterDetail;
import uk.gov.gchq.gaffer.named.operation.cache.exception.CacheOperationFailedException;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.named.cache.NamedOperationCache;
import uk.gov.gchq.gaffer.user.User;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class AddNamedOperationHandlerTest {
    private final NamedOperationCache mockCache = mock(NamedOperationCache.class);
    private final AddNamedOperationHandler handler = new AddNamedOperationHandler(mockCache);

    private Context context = new Context(new User.Builder()
            .userId("test user")
            .build());
    private Store store = mock(Store.class);

    private AddNamedOperation addNamedOperation = new AddNamedOperation.Builder()
            .overwrite(false)
            .build();
    private static final String OPERATION_NAME = "test";
    private HashMap<String, NamedOperationDetail> storedOperations = new HashMap<>();

    @Before
    public void before() throws CacheOperationFailedException {
        storedOperations.clear();
        addNamedOperation.setOperationName(OPERATION_NAME);

        doAnswer(invocationOnMock -> {
            Object[] args = invocationOnMock.getArguments();
            storedOperations.put(((NamedOperationDetail) args[0]).getOperationName(), (NamedOperationDetail) args[0]);
            return null;
        }).when(mockCache).addNamedOperation(any(NamedOperationDetail.class), anyBoolean(), any(User.class));

        doAnswer(invocationOnMock ->
                new WrappedCloseableIterable<>(storedOperations.values()))
                .when(mockCache).getAllNamedOperations(any(User.class));

        doAnswer(invocationOnMock -> {
            String name = (String) invocationOnMock.getArguments()[0];
            NamedOperationDetail result = storedOperations.get(name);
            if (result == null) {
                throw new CacheOperationFailedException();
            }
            return result;
        }).when(mockCache).getNamedOperation(anyString(), any(User.class));
    }

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @After
    public void after() throws CacheOperationFailedException {
        addNamedOperation.setOperationName(null);
        addNamedOperation.setOperationChain((String) null);
        addNamedOperation.setDescription(null);
        addNamedOperation.setOverwriteFlag(false);
        mockCache.clear();
    }


    @Test
    public void shouldNotAllowForNonRecursiveNamedOperationsToBeNested() throws OperationException {
        OperationChain child = new OperationChain.Builder().first(new AddElements()).build();
        addNamedOperation.setOperationChain(child);
        addNamedOperation.setOperationName("child");
        handler.doOperation(addNamedOperation, context, store);

        OperationChain parent = new OperationChain.Builder()
                .first(new NamedOperation.Builder().name("child").build())
                .then(new GetElements())
                .build();

        addNamedOperation.setOperationChain(parent);
        addNamedOperation.setOperationName("parent");

        exception.expect(OperationException.class);

        handler.doOperation(addNamedOperation, context, store);
    }

    @Test
    public void shouldAllowForOperationChainJSONWithParameter() {
        try {
            final String opChainJSON = "{ \"operations\": [ { \"class\":\"uk.gov.gchq.gaffer.operation.impl.get.GetAllElements\" }, { \"class\":\"uk.gov.gchq.gaffer.operation.impl.Limit\", \"resultLimit\": \"${param1}\" } ] }";

            addNamedOperation.setOperationChain(opChainJSON);
            addNamedOperation.setOperationName("namedop");
            ParameterDetail param = new ParameterDetail.Builder()
                    .defaultValue(1L)
                    .description("Limit param")
                    .valueClass(Long.class)
                    .build();
            Map<String, ParameterDetail> paramMap = Maps.newHashMap();
            paramMap.put("param1", param);
            addNamedOperation.setParameters(paramMap);
            handler.doOperation(addNamedOperation, context, store);
            assert cacheContains("namedop");

        } catch (final Exception e) {
            fail("Expected test to pass without error. Exception " + e.getMessage());
        }

    }

    @Test
    public void shouldNotAllowForOperationChainWithParameterNotInOperationString() throws OperationException {
        final String opChainJSON = "{ \"operations\": [ { \"class\":\"uk.gov.gchq.gaffer.operation.impl.get.GetAllElements\" }, { \"class\":\"uk.gov.gchq.gaffer.operation.impl.export.set.ExportToSet\", \"key\": \"${param1}\" } ] }";

        addNamedOperation.setOperationChain(opChainJSON);
        addNamedOperation.setOperationName("namedop");

        // Note the param is String class to get past type checking which will also catch a param
        // with an unknown name if its not a string.
        ParameterDetail param = new ParameterDetail.Builder()
                .defaultValue("setKey")
                .description("key param")
                .valueClass(String.class)
                .build();
        Map<String, ParameterDetail> paramMap = Maps.newHashMap();
        paramMap.put("param2", param);
        addNamedOperation.setParameters(paramMap);

        exception.expect(OperationException.class);
        handler.doOperation(addNamedOperation, context, store);
    }

    @Test
    public void shouldNotAllowForOperationChainJSONWithInvalidParameter() throws UnsupportedEncodingException, SerialisationException {
        String opChainJSON = "{" +
                "  \"operations\": [" +
                "      {" +
                "          \"class\": \"uk.gov.gchq.gaffer.named.operation.AddNamedOperation\"," +
                "          \"operationName\": \"testInputParam\"," +
                "          \"overwriteFlag\": true," +
                "          \"operationChain\": {" +
                "              \"operations\": [" +
                "                  {" +
                "                      \"class\": \"uk.gov.gchq.gaffer.operation.impl.get.GetAllElements\"" +
                "                  }," +
                "                  {" +
                "                     \"class\": \"uk.gov.gchq.gaffer.operation.impl.Limit\"," +
                "                     \"resultLimit\": \"${param1}\"" +
                "                  }" +
                "              ]" +
                "           }," +
                "           \"parameters\": {" +
                "               \"param1\" : { \"description\" : \"Test Long parameter\"," +
                "                              \"defaultValue\" : [ \"bad arg type\" ]," +
                "                              \"requiredArg\" : false," +
                "                              \"valueClass\": \"java.lang.Long\"" +
                "                          }" +
                "           }" +
                "       }" +
                "   ]" +
                "}";

        exception.expect(SerialisationException.class);
        JSONSerialiser.deserialise(opChainJSON.getBytes("UTF-8"), OperationChain.class);
    }

    @Test
    public void shouldAddNamedOperationWithScoreCorrectly() throws OperationException, CacheOperationFailedException {
        OperationChain opChain = new OperationChain.Builder().first(new AddElements()).build();
        addNamedOperation.setOperationChain(opChain);
        addNamedOperation.setScore(2);
        addNamedOperation.setOperationName("testOp");

        handler.doOperation(addNamedOperation, context, store);

        final NamedOperationDetail result = mockCache.getNamedOperation("testOp", new User());

        assert cacheContains("testOp");
        assertEquals(addNamedOperation.getScore(), result.getScore());
    }

    private boolean cacheContains(final String opName) {
        Iterable<NamedOperationDetail> ops = mockCache.getAllNamedOperations(context.getUser());
        for (final NamedOperationDetail op : ops) {
            if (op.getOperationName().equals(opName)) {
                return true;
            }
        }
        return false;

    }
}
