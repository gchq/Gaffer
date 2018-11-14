/*
 * Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.store;

import com.fasterxml.jackson.annotation.JsonInclude;

import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationChainDAO;
import uk.gov.gchq.gaffer.operation.VariableDetail;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class VariablesResolver {
    private static final String CHARSET_NAME = CommonConstants.UTF_8;

    public List<Operation> resolve(final OperationChain<?> opChain, final Context context) {
        String opChainAsString = getOpChainAsString(opChain);
        return resolveOperationsVariables(opChainAsString, context);
    }

    public List<Operation> resolveOperationsVariables(final String opChainInput, final Context context) {
        String opChainString = opChainInput;
        final Map<String, VariableDetail> variableMap = getAllVariableToVariableDetailMapFromOpChain(opChainString, context);

        if (null != variableMap) {
            for (final Map.Entry<String, VariableDetail> variablePair : variableMap.entrySet()) {
                String variablePairName = variablePair.getKey();

                try {
                    if (variablePair.getValue() == null) {
                        opChainString = opChainString.replace(buildParamNameString(variablePairName),
                                new String(JSONSerialiser.serialise("\"null\"", CHARSET_NAME), CHARSET_NAME));
                    } else {
                        Object paramObj = JSONSerialiser.deserialise(JSONSerialiser.serialise(variablePair.getValue().getValue()), variablePair.getValue().getValueClass());

                        opChainString = opChainString.replace(buildParamNameString(variablePairName),
                                new String(JSONSerialiser.serialise(paramObj, CHARSET_NAME), CHARSET_NAME));
                    }
                } catch (final SerialisationException | UnsupportedEncodingException e) {
                    throw new IllegalArgumentException(e.getMessage());
                }
            }
        }

        final OperationChain opChain;

        try {
            opChain = JSONSerialiser.deserialise(opChainString.getBytes(CHARSET_NAME), OperationChainDAO.class);
        } catch (final Exception e) {
            throw new IllegalArgumentException(e.getMessage());
        }
        return opChain.getOperations();
    }

    private String buildParamNameString(final String paramKey) {
        return "\"${" + paramKey + "}\"";
    }

    private Map<String, VariableDetail> getAllVariableToVariableDetailMapFromOpChain(final String opChain, final Context context) {

        final String patternString = "\\$\\{(.*?)\\}";
        final Pattern pattern = Pattern.compile(patternString);
        final Matcher matcher = pattern.matcher(opChain);

        final Map<String, VariableDetail> matches = new HashMap<>();
        while (matcher.find()) {
            matches.put(matcher.group(1), resolveVariableDetailFromVariableName(matcher.group(1), context));
        }
        return matches;
    }

    private VariableDetail resolveVariableDetailFromVariableName(final String variableName, final Context context) {
        return context.getVariable(variableName) != null ? context.getVariable(variableName) : null;
    }

    private String getOpChainAsString(final OperationChain<?> opChain) {
        try {
            return new String(JSONSerialiser.serialise(opChain), Charset.forName(CHARSET_NAME));
        } catch (final SerialisationException se) {
            throw new IllegalArgumentException(se.getMessage());
        }
    }
}
