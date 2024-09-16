/*
 * Copyright 2017-2023 Crown Copyright
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
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.gchq.gaffer.access.predicate.AccessPredicate;
import uk.gov.gchq.gaffer.access.predicate.user.CustomUserPredicate;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.named.operation.AddNamedOperation;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.named.operation.NamedOperationDetail;
import uk.gov.gchq.gaffer.named.operation.ParameterDetail;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.operation.handler.named.cache.NamedOperationCache;
import uk.gov.gchq.gaffer.user.User;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
public class AddNamedOperationHandlerTest {

    private static final String EMPTY_ADMIN_AUTH = "";
    private static final String OPERATION_NAME = "test";

    private final Context context = new Context(new User.Builder()
            .userId("test user")
            .build());

    private final AddNamedOperation addNamedOperation = new AddNamedOperation.Builder()
            .overwrite(false)
            .build();

    private final HashMap<String, NamedOperationDetail> storedOperations = new HashMap<>();

    @Mock
    private NamedOperationCache mockCache;
    @Mock
    private Store store;

    private AddNamedOperationHandler handler;

    @BeforeEach
    public void before() throws CacheOperationException {
        storedOperations.clear();
        handler = new AddNamedOperationHandler(mockCache, true);

        addNamedOperation.setOperationName(OPERATION_NAME);

        lenient().doAnswer(invocationOnMock -> {
            final Object[] args = invocationOnMock.getArguments();
            storedOperations.put(((NamedOperationDetail) args[0]).getOperationName(), (NamedOperationDetail) args[0]);
            return null;
        }).when(mockCache).addNamedOperation(any(NamedOperationDetail.class), anyBoolean(), any(User.class), eq(EMPTY_ADMIN_AUTH));

        lenient().doAnswer(invocationOnMock -> storedOperations.values())
                .when(mockCache).getAllNamedOperations(any(User.class), eq(EMPTY_ADMIN_AUTH));

        lenient().doAnswer(invocationOnMock -> {
            final String name = (String) invocationOnMock.getArguments()[0];
            final NamedOperationDetail result = storedOperations.get(name);
            if (result == null) {
                throw new CacheOperationException();
            }
            return result;
        }).when(mockCache).getNamedOperation(anyString(), any(User.class), eq(EMPTY_ADMIN_AUTH));

        lenient().when(store.getProperties()).thenReturn(new StoreProperties());
    }

    @AfterEach
    public void after() throws CacheOperationException {
        addNamedOperation.setOperationName(null);
        addNamedOperation.setOperationChain((String) null);
        addNamedOperation.setDescription(null);
        addNamedOperation.setOverwriteFlag(false);
        addNamedOperation.setReadAccessPredicate(null);
        addNamedOperation.setWriteAccessPredicate(null);
        storedOperations.clear();
        mockCache.clearCache();
    }

    @SuppressWarnings({"rawtypes"})
    @Test
    public void shouldNotAllowForNonRecursiveNamedOperationsToBeNested() throws OperationException {
        final OperationChain<?> child = new OperationChain.Builder().first(new AddElements()).build();
        addNamedOperation.setOperationChain(child);
        addNamedOperation.setOperationName("child");

        //isNestedNamedOperationsAllowed = FALSE
        handler = new AddNamedOperationHandler(mockCache, false);
        handler.doOperation(addNamedOperation, context, store);

        final OperationChain<?> parent = new OperationChain.Builder()
                .first(new NamedOperation.Builder().name("child").build())
                .then(new GetElements())
                .build();

        addNamedOperation.setOperationChain(parent);
        addNamedOperation.setOperationName("parent");

        assertThatExceptionOfType(OperationException.class).isThrownBy(() -> handler.doOperation(addNamedOperation, context, store));
    }

    @Test
    public void shouldAllowForRecursiveNamedOperationsToBeNested() throws OperationException {
        final OperationChain<?> child = new OperationChain.Builder().first(new AddElements()).build();
        addNamedOperation.setOperationChain(child);
        addNamedOperation.setOperationName("child");

        //isNestedNamedOperationsAllowed = TRUE
        handler = new AddNamedOperationHandler(mockCache, true);
        handler.doOperation(addNamedOperation, context, store);

        final OperationChain<?> parent = new OperationChain.Builder()
                .first(new NamedOperation.Builder().name("child").build())
                .then(new GetElements())
                .build();

        addNamedOperation.setOperationChain(parent);
        addNamedOperation.setOperationName("parent");

        handler.doOperation(addNamedOperation, context, store);
    }


    @Test
    public void shouldAllowForOperationChainJSONWithParameter() throws OperationException {
        final String opChainJSON = "{ \"operations\": [ { \"class\":\"uk.gov.gchq.gaffer.operation.impl.get.GetAllElements\" }, "
                + "{ \"class\":\"uk.gov.gchq.gaffer.operation.impl.Limit\", \"resultLimit\": \"${param1}\" } ] }";

        addNamedOperation.setOperationChain(opChainJSON);
        addNamedOperation.setOperationName("namedop");
        final ParameterDetail param = new ParameterDetail.Builder()
                .defaultValue(1L)
                .description("Limit param")
                .valueClass(Long.class)
                .build();
        final Map<String, ParameterDetail> paramMap = Maps.newHashMap();
        paramMap.put("param1", param);
        addNamedOperation.setParameters(paramMap);
        handler.doOperation(addNamedOperation, context, store);
        assertThat(cacheContains("namedop")).isTrue();
    }

    @Test
    public void shouldNotAllowForOperationChainWithParameterNotInOperationString() throws OperationException {
        final String opChainJSON = "{ \"operations\": [ { \"class\":\"uk.gov.gchq.gaffer.operation.impl.get.GetAllElements\" }, "
                + "{ \"class\":\"uk.gov.gchq.gaffer.operation.impl.export.set.ExportToSet\", \"key\": \"${param1}\" } ] }";

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
        addNamedOperation.setParameters(paramMap);

        assertThatExceptionOfType(OperationException.class).isThrownBy(() -> handler.doOperation(addNamedOperation, context, store));
    }

    @Test
    public void shouldNotAllowForOperationChainJSONWithInvalidParameter() throws SerialisationException {
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

        assertThatExceptionOfType(SerialisationException.class)
                .isThrownBy(() -> JSONSerialiser.deserialise(opChainJSON.getBytes(StandardCharsets.UTF_8), OperationChain.class));
    }

    @Test
    public void shouldAddNamedOperationFieldsToNamedOperationDetailCorrectly() throws OperationException, CacheOperationException {
        final List<String> readAuths = asList("readAuth1", "readAuth2");
        final List<String> writeAuths = asList("writeAuth1", "writeAuth2");
        final OperationChain<?> opChain = new OperationChain.Builder().first(new AddElements()).build();
        addNamedOperation.setOperationChain(opChain);
        addNamedOperation.setScore(2);
        addNamedOperation.setOperationName("testOp");
        addNamedOperation.setLabels(asList("test label"));
        addNamedOperation.setReadAccessRoles(readAuths);
        addNamedOperation.setWriteAccessRoles(writeAuths);

        handler.doOperation(addNamedOperation, context, store);

        final NamedOperationDetail result = mockCache.getNamedOperation("testOp", new User(), EMPTY_ADMIN_AUTH);

        assertThat(cacheContains("testOp")).isTrue();

        assertThat(result.getScore()).isEqualTo(2);
        assertThat(result.getLabels()).containsExactly("test label");

        final AccessPredicate expectedReadAccessPredicate = new AccessPredicate(context.getUser(), readAuths);
        assertThat(result.getOrDefaultReadAccessPredicate()).isEqualTo(expectedReadAccessPredicate);

        final AccessPredicate expectedWriteAccessPredicate = new AccessPredicate(context.getUser(), writeAuths);
        assertThat(result.getOrDefaultWriteAccessPredicate()).isEqualTo(expectedWriteAccessPredicate);
    }

    @Test
    public void shouldAddCustomAccessPredicateFieldsToNamedOperationDetailCorrectly()
            throws OperationException, CacheOperationException {
        final AccessPredicate readAccessPredicate = new AccessPredicate(new CustomUserPredicate());
        final AccessPredicate writeAccessPredicate = new AccessPredicate(new CustomUserPredicate());
        final OperationChain<?> opChain = new OperationChain.Builder().first(new AddElements()).build();
        addNamedOperation.setOperationChain(opChain);
        addNamedOperation.setScore(2);
        addNamedOperation.setOperationName("testOp");
        addNamedOperation.setLabels(asList("test label"));
        addNamedOperation.setReadAccessRoles(null);
        addNamedOperation.setReadAccessPredicate(readAccessPredicate);
        addNamedOperation.setWriteAccessRoles(null);
        addNamedOperation.setWriteAccessPredicate(writeAccessPredicate);

        handler.doOperation(addNamedOperation, context, store);

        final NamedOperationDetail result = mockCache.getNamedOperation("testOp", new User(), EMPTY_ADMIN_AUTH);

        assertThat(cacheContains("testOp")).isTrue();

        assertThat(result.getScore()).isEqualTo(2);
        assertThat(result.getLabels()).containsExactly("test label");
        assertThat(result.getReadAccessPredicate()).isEqualTo(readAccessPredicate);
        assertThat(result.getWriteAccessPredicate()).isEqualTo(writeAccessPredicate);
    }

    private boolean cacheContains(final String operationName) {
        final Iterable<NamedOperationDetail> ops = mockCache.getAllNamedOperations(context.getUser(), EMPTY_ADMIN_AUTH);
        for (final NamedOperationDetail op : ops) {
            if (op.getOperationName().equals(operationName)) {
                return true;
            }
        }
        return false;
    }
}
