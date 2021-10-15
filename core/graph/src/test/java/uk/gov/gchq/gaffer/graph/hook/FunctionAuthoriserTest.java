/*
 * Copyright 2020-2021 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
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

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.spy;

public class FunctionAuthoriserTest extends GraphHookTest<FunctionAuthoriser> {

    private static final String JSON_PATH = "/functionAuthoriser.json";

    public FunctionAuthoriserTest() {
        super(FunctionAuthoriser.class);
    }

    @Test
    public void shouldNotAllowOperationWhichContainsUnauthorisedFunction() {
        // Given
        OperationChain badOperation = generateOperation(Identity.class);
        FunctionAuthoriser functionAuthoriser = new FunctionAuthoriser();

        // When
        functionAuthoriser.setUnauthorisedFunctions(Lists.newArrayList(Identity.class));

        // Then
        assertThatExceptionOfType(UnauthorisedException.class)
                .isThrownBy(() -> functionAuthoriser.preExecute(badOperation, new Context()))
                .withMessage("Operation chain contained an unauthorised function: uk.gov.gchq.koryphe.impl.function.Identity");
    }

    @Test
    public void shouldNotAllowGetElementsOperationWithUnauthorisedFunctionsInTheView() {
        final OperationChain<CloseableIterable<? extends Element>> viewOperation = new OperationChain.Builder().first(new GetElements.Builder()
                .view(new View.Builder()
                        .globalElements(new GlobalViewElementDefinition.Builder()
                                .transformFunctions(Lists.newArrayList(new TupleAdaptedFunction(new String[]{"input"}, new DivideBy(6), new String[]{"output"})))
                                .build())
                        .build())
                .build())
                .build();

        FunctionAuthoriser functionAuthoriser = new FunctionAuthoriser();

        // When
        functionAuthoriser.setUnauthorisedFunctions(Lists.newArrayList(DivideBy.class));

        // Then
        assertThatExceptionOfType(UnauthorisedException.class)
                .isThrownBy(() -> functionAuthoriser.preExecute(viewOperation, new Context()))
                .withMessage("Operation chain contained an unauthorised function: uk.gov.gchq.koryphe.impl.function.DivideBy");

    }

    @Test
    public void shouldAllowOperationChainWhichDoesNotContainAnyUnauthorisedElements() {
        // Given
        OperationChain mapOperation = generateOperation(Identity.class, ToString.class);
        FunctionAuthoriser functionAuthoriser = new FunctionAuthoriser();

        // When
        functionAuthoriser.setUnauthorisedFunctions(Lists.newArrayList(DivideBy.class));

        // Then
        functionAuthoriser.preExecute(mapOperation, new Context());
        // No exceptions thrown
    }

    @Test
    public void shouldAllowEmptyOperationChain() {
        // Given
        OperationChain chain = new OperationChain();

        // When
        FunctionAuthoriser authoriser = new FunctionAuthoriser(Lists.newArrayList(Identity.class));

        // Then no exceptions
        authoriser.preExecute(chain, new Context());
    }

    @Test
    public void shouldAllowAllFunctionsWhenUnauthorisedFunctionsAreNull() {
        // Given
        OperationChain chain = generateOperation(Identity.class, ToString.class);

        // When
        FunctionAuthoriser authoriser = new FunctionAuthoriser(null);

        // Then no exceptions
        authoriser.preExecute(chain, new Context());
    }

    @Test
    public void shouldAllowAllFunctionsWhenUnauthorisedFunctionsAreEmpty() {
        // Given
        OperationChain chain = generateOperation(Identity.class, ToString.class);

        // When
        FunctionAuthoriser authoriser = new FunctionAuthoriser(new ArrayList<>());

        // Then no exceptions
        authoriser.preExecute(chain, new Context());
    }

    @Test
    public void shouldNotErrorIfFirstOperationIsNotInput() {
        // Given
        OperationChain chain = new OperationChain.Builder()
                .first(new GetAllElements()).build();

        // When
        FunctionAuthoriser authoriser = new FunctionAuthoriser(Lists.newArrayList(Identity.class));

        // Then no exceptions
        authoriser.preExecute(chain, new Context());
    }

    @Test
    public void shouldWorkIfUsingShortClassNames() {
        // Given
        OperationChain badOperation = generateOperation(Identity.class);
        FunctionAuthoriser functionAuthoriser = new FunctionAuthoriser();

        // When
        SimpleClassNameCache.setUseFullNameForSerialisation(false);
        functionAuthoriser.setUnauthorisedFunctions(Lists.newArrayList(Identity.class));

        // Then
        assertThatExceptionOfType(UnauthorisedException.class)
                .isThrownBy(() -> functionAuthoriser.preExecute(badOperation, new Context()))
                .withMessage("Operation chain contained an unauthorised function: uk.gov.gchq.koryphe.impl.function.Identity");
    }

    @Test
    public void shouldJsonSerialiseAndDeserialiseWithPopulatedFields() throws SerialisationException {
        String json = "" +
                "{" +
                "\"class\": \"uk.gov.gchq.gaffer.graph.hook.FunctionAuthoriser\"," +
                "\"unauthorisedFunctions\":[" +
                "\"uk.gov.gchq.koryphe.impl.function.ToString\"" +
                "]" +
                "}";

        final FunctionAuthoriser authoriser = new FunctionAuthoriser(Lists.newArrayList(ToString.class));

        JsonAssert.assertEquals(json, new String(JSONSerialiser.serialise(authoriser)));
    }

    @Test
    public void shouldJsonSerialiseAndDeserialiseWithNoFields() throws SerialisationException {
        String json = "" +
                "{" +
                "\"class\": \"uk.gov.gchq.gaffer.graph.hook.FunctionAuthoriser\"" +
                "}";

        final FunctionAuthoriser authoriser = new FunctionAuthoriser();

        JsonAssert.assertEquals(json, new String(JSONSerialiser.serialise(authoriser)));
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
