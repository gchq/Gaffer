/*
 * Copyright 2020 Crown Copyright
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

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.exception.UnauthorisedException;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.GlobalViewElementDefinition;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.function.ToEntityId;
import uk.gov.gchq.gaffer.operation.impl.Map;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.koryphe.impl.function.DivideBy;
import uk.gov.gchq.koryphe.impl.function.Identity;
import uk.gov.gchq.koryphe.impl.function.ToString;
import uk.gov.gchq.koryphe.serialisation.json.SimpleClassNameCache;
import uk.gov.gchq.koryphe.tuple.function.TupleAdaptedFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.spy;
import static uk.gov.gchq.gaffer.commonutil.JsonAssert.assertJsonEquals;

public class FunctionAuthoriserTest extends GraphHookTest<FunctionAuthoriser> {

    private static final String JSON_PATH = "/functionAuthoriser.json";

    public FunctionAuthoriserTest() {
        super(FunctionAuthoriser.class);
    }

    @Test
    public void shouldNotAllowOperationWhichContainsUnauthorisedFunction() {
        final OperationChain badOperation = generateOperation(Identity.class);
        final FunctionAuthoriser functionAuthoriser = new FunctionAuthoriser();

        functionAuthoriser.setUnauthorisedFunctions(Lists.newArrayList(Identity.class));

        final Exception exception = assertThrows(UnauthorisedException.class, () -> functionAuthoriser.preExecute(badOperation, new Context()));
        assertEquals("Operation chain contained an unauthorised function: uk.gov.gchq.koryphe.impl.function.Identity", exception.getMessage());
    }

    @Test
    public void shouldNotAllowGetElementsOperationWithUnauthorisedFunctionsInTheView() {
        final View viewWithUnauthorisedFunction = new View.Builder()
                .globalElements(new GlobalViewElementDefinition.Builder()
                        .transformFunctions(Lists.newArrayList(new TupleAdaptedFunction(new String[] {"input"}, new DivideBy(6), new String[] {"output"})))
                        .build())
                .build();
        final OperationChain<CloseableIterable<? extends Element>> viewOperation = new OperationChain.Builder().first(new GetElements.Builder()
                .view(viewWithUnauthorisedFunction)
                .build())
                .build();

        final FunctionAuthoriser functionAuthoriser = new FunctionAuthoriser();

        // When
        functionAuthoriser.setUnauthorisedFunctions(Lists.newArrayList(DivideBy.class));

        final Exception exception = assertThrows(UnauthorisedException.class, () -> functionAuthoriser.preExecute(viewOperation, new Context()));
        assertEquals("Operation chain contained an unauthorised function: uk.gov.gchq.koryphe.impl.function.DivideBy", exception.getMessage());
    }

    @Test
    public void shouldAllowOperationChainWhichDoesNotContainAnyUnauthorisedElements() {
        final OperationChain mapOperation = generateOperation(Identity.class, ToString.class);
        final FunctionAuthoriser functionAuthoriser = new FunctionAuthoriser();

        functionAuthoriser.setUnauthorisedFunctions(Lists.newArrayList(DivideBy.class));

        assertDoesNotThrow(() -> functionAuthoriser.preExecute(mapOperation, new Context()));
    }

    @Test
    public void shouldAllowEmptyOperationChain() {
        final OperationChain chain = new OperationChain();

        final FunctionAuthoriser authoriser = new FunctionAuthoriser(Lists.newArrayList(Identity.class));

        assertDoesNotThrow(() -> authoriser.preExecute(chain, new Context()));
    }

    @Test
    public void shouldAllowAllFunctionsWhenUnauthorisedFunctionsAreNull() {
        final OperationChain chain = generateOperation(Identity.class, ToString.class);

        final FunctionAuthoriser authoriser = new FunctionAuthoriser(null);

        assertDoesNotThrow(() -> authoriser.preExecute(chain, new Context()));
    }

    @Test
    public void shouldAllowAllFunctionsWhenUnauthorisedFunctionsAreEmpty() {
        final OperationChain chain = generateOperation(Identity.class, ToString.class);

        final FunctionAuthoriser authoriser = new FunctionAuthoriser(new ArrayList<>());

        assertDoesNotThrow(() -> authoriser.preExecute(chain, new Context()));
    }

    @Test
    public void shouldNotErrorIfFirstOperationIsNotInput() {
        final OperationChain chain = new OperationChain.Builder()
                .first(new GetAllElements()).build();

        final FunctionAuthoriser authoriser = new FunctionAuthoriser(Lists.newArrayList(Identity.class));

        assertDoesNotThrow(() -> authoriser.preExecute(chain, new Context()));
    }

    @Test
    public void shouldWorkIfUsingShortClassNames() {
        // Given
        final OperationChain badOperation = generateOperation(Identity.class);
        final FunctionAuthoriser functionAuthoriser = new FunctionAuthoriser();

        // When
        SimpleClassNameCache.setUseFullNameForSerialisation(false);
        functionAuthoriser.setUnauthorisedFunctions(Lists.newArrayList(Identity.class));

        // Then
        final Exception exception = assertThrows(UnauthorisedException.class, () -> functionAuthoriser.preExecute(badOperation, new Context()));
        assertEquals("Operation chain contained an unauthorised function: uk.gov.gchq.koryphe.impl.function.Identity", exception.getMessage());
    }

    @Test
    public void shouldJsonSerialiseAndDeserialiseWithPopulatedFields() throws SerialisationException {
        final String json = "{" +
                "\"class\": \"uk.gov.gchq.gaffer.graph.hook.FunctionAuthoriser\"," +
                "\"unauthorisedFunctions\":[" +
                "\"uk.gov.gchq.koryphe.impl.function.ToString\"" +
                "]" +
                "}";

        final FunctionAuthoriser authoriser = new FunctionAuthoriser(Lists.newArrayList(ToString.class));

        assertJsonEquals(json, new String(JSONSerialiser.serialise(authoriser)));
    }

    @Test
    public void shouldJsonSerialiseAndDeserialiseWithNoFields() throws SerialisationException {
        final String json = "{" +
                "\"class\": \"uk.gov.gchq.gaffer.graph.hook.FunctionAuthoriser\"" +
                "}";

        final FunctionAuthoriser authoriser = new FunctionAuthoriser();

        assertJsonEquals(json, new String(JSONSerialiser.serialise(authoriser)));
    }

    @Test
    public void shouldMaintainOperationChainIfItFailsToSerialise() {
        // Given
        FunctionAuthoriser authoriser = new FunctionAuthoriser(Lists.newArrayList(Identity.class));

        List fakeInput = Lists.newArrayList(new EntitySeed(1), new EntitySeed(2), new EntitySeed(3));
        GetElements getElements = new GetElements();
        getElements.setInput(fakeInput);

        getElements = spy(getElements); // will fail serialisation

        final OperationChain chain = new OperationChain.Builder()
                .first(getElements)
                .then(generateOperation(ToEntityId.class))
                .build();

        // When
        authoriser.preExecute(chain, new Context());

        // Then
        assertEquals(fakeInput, ((Input) chain.getOperations().get(0)).getInput());
    }

    @Test
    public void shouldMaintainOperationChainInputIfItSerialises() {
        // Given
        FunctionAuthoriser authoriser = new FunctionAuthoriser(Lists.newArrayList(Identity.class));

        List fakeInput = Lists.newArrayList(new EntitySeed(1), new EntitySeed(2), new EntitySeed(3));
        GetElements getElements = new GetElements();
        getElements.setInput(fakeInput);

        final OperationChain chain = new OperationChain.Builder()
                .first(getElements)
                .then(generateOperation(ToEntityId.class))
                .build();

        // When
        authoriser.preExecute(chain, new Context());


        // Then
        assertEquals(fakeInput, ((Input) chain.getOperations().get(0)).getInput());
    }

    private OperationChain generateOperation(final Class<? extends Function>... functionClasses) {
        final Map.Builder builder = new Map.Builder();
        try {
            final Map.OutputBuilder mob = builder.first(functionClasses[0].newInstance());

            boolean first = true;
            for (Class<? extends Function> functionClass : functionClasses) {
                if (first) {
                    first = false;
                    continue;
                }
                mob.then(functionClass.newInstance());
            }

            return new OperationChain(mob.build());

        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected FunctionAuthoriser getTestObject() {
        return fromJson(JSON_PATH);
    }
}
