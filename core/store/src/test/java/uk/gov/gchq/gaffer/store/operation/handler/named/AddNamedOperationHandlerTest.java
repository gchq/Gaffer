/*
 * Copyright 2016-2020 Crown Copyright
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.exception.SerialisationException;
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
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.operation.handler.named.cache.NamedOperationCache;
import uk.gov.gchq.gaffer.user.User;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser.deserialise;

public class AddNamedOperationHandlerTest {

    private static final String EMPTY_ADMIN_AUTH = "";
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

    @BeforeEach
    public void before() throws CacheOperationFailedException {
        storedOperations.clear();
        addNamedOperation.setOperationName(OPERATION_NAME);

        doAnswer(invocationOnMock -> {
            Object[] args = invocationOnMock.getArguments();
            storedOperations.put(((NamedOperationDetail) args[0]).getOperationName(), (NamedOperationDetail) args[0]);
            return null;
        }).when(mockCache).addNamedOperation(any(NamedOperationDetail.class), anyBoolean(), any(User.class), eq(EMPTY_ADMIN_AUTH));

        doAnswer(invocationOnMock ->
                new WrappedCloseableIterable<>(storedOperations.values()))
                .when(mockCache).getAllNamedOperations(any(User.class), eq(EMPTY_ADMIN_AUTH));

        doAnswer(invocationOnMock -> {
            String name = (String) invocationOnMock.getArguments()[0];
            NamedOperationDetail result = storedOperations.get(name);
            if (result == null) {
                throw new CacheOperationFailedException();
            }
            return result;
        }).when(mockCache).getNamedOperation(anyString(), any(User.class), eq(EMPTY_ADMIN_AUTH));

        given(store.getProperties()).willReturn(new StoreProperties());
    }

    @AfterEach
    public void after() throws CacheOperationFailedException {
        addNamedOperation.setOperationName(null);
        addNamedOperation.setOperationChain((String) null);
        addNamedOperation.setDescription(null);
        addNamedOperation.setOverwriteFlag(false);
        mockCache.clear();
    }

    @Test
    public void shouldNotAllowForNonRecursiveNamedOperationsToBeNested() throws OperationException {
        // Given
        OperationChain child = new OperationChain.Builder().first(new AddElements()).build();
        addNamedOperation.setOperationChain(child);
        addNamedOperation.setOperationName("child");
        handler.doOperation(addNamedOperation, context, store);

        OperationChain parent = new OperationChain.Builder()
                .first(new NamedOperation.Builder().name("child").build())
                .then(new GetElements())
                .build();

        // When
        addNamedOperation.setOperationChain(parent);
        addNamedOperation.setOperationName("parent");

        // Then
        assertThrows(OperationException.class, () -> handler.doOperation(addNamedOperation, context, store));
    }

    @Test
    public void shouldAllowForOperationChainJSONWithParameter() {
        // Given
        final String opChainJSON = "{ \"operations\": [ { \"class\":\"uk.gov.gchq.gaffer.operation.impl.get.GetAllElements\" }, { \"class\":\"uk.gov.gchq.gaffer.operation.impl.Limit\", \"resultLimit\": \"${param1}\" } ] }";

        addNamedOperation.setOperationChain(opChainJSON);
        addNamedOperation.setOperationName("namedop");

        // When
        final ParameterDetail param = new ParameterDetail.Builder()
                .defaultValue(1L)
                .description("Limit param")
                .valueClass(Long.class)
                .build();
        Map<String, ParameterDetail> paramMap = Maps.newHashMap();
        paramMap.put("param1", param);
        addNamedOperation.setParameters(paramMap);

        // Then
        assertDoesNotThrow(() -> handler.doOperation(addNamedOperation, context, store));
        assert cacheContains("namedop");
    }

    @Test
    public void shouldNotAllowForOperationChainWithParameterNotInOperationString() {
        // Given
        final String opChainJSON = "{ \"operations\": [ { \"class\":\"uk.gov.gchq.gaffer.operation.impl.get.GetAllElements\" }, { \"class\":\"uk.gov.gchq.gaffer.operation.impl.export.set.ExportToSet\", \"key\": \"${param1}\" } ] }";

        addNamedOperation.setOperationChain(opChainJSON);
        addNamedOperation.setOperationName("namedop");

        // Note the param is String class to get past type checking which will also catch a param
        // with an unknown name if its not a string.
        final ParameterDetail param = new ParameterDetail.Builder()
                .defaultValue("setKey")
                .description("key param")
                .valueClass(String.class)
                .build();
        final Map<String, ParameterDetail> paramMap = Maps.newHashMap();
        paramMap.put("param2", param);

        // When
        addNamedOperation.setParameters(paramMap);

        // Then
        assertThrows(OperationException.class, () -> handler.doOperation(addNamedOperation, context, store));
    }

    @Test
    public void shouldNotAllowForOperationChainJSONWithInvalidParameter() {
        // Given
        final String opChainJSON = "{" +
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

        // When / Then
        assertThrows(SerialisationException.class, () -> deserialise(opChainJSON.getBytes("UTF-8"), OperationChain.class));
    }

    @Test
    public void shouldAddNamedOperationFieldsToNamedOperationDetailCorrectly() throws CacheOperationFailedException {
        // Given
        OperationChain opChain = new OperationChain.Builder().first(new AddElements()).build();

        // When
        addNamedOperation.setOperationChain(opChain);
        addNamedOperation.setScore(2);
        addNamedOperation.setOperationName("testOp");
        addNamedOperation.setLabels(Arrays.asList("test label"));

        // Then
        assertDoesNotThrow(() -> handler.doOperation(addNamedOperation, context, store));

        final NamedOperationDetail result = mockCache.getNamedOperation("testOp", new User(), EMPTY_ADMIN_AUTH);

        assertTrue(cacheContains("testOp"));
        assertTrue(result.getScore() == 2);
        assertEquals(Arrays.asList("test label"), result.getLabels());
    }

    private boolean cacheContains(final String operationName) {
        Iterable<NamedOperationDetail> ops = mockCache.getAllNamedOperations(context.getUser(), EMPTY_ADMIN_AUTH);
        for (final NamedOperationDetail op : ops) {
            if (op.getOperationName().equals(operationName)) {
                return true;
            }
        }
        return false;
    }
}
