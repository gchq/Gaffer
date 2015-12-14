/**
 * Copyright 2015 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gaffer.analytic;

import gaffer.analytic.parameters.Parameters;
import gaffer.analytic.result.Result;

import java.io.IOException;

/**
 * Simple interface to provide some consistency for implementations of analytics.
 */
public interface Analytic {

	/**
	 * Returns the name of the analytic, e.g. "shortest path".
	 * 
	 * @return
	 */
	String getName();
	
	/**
	 * Returns a short description of the analytic.
	 * 
	 * @return
	 */
	String getDescription();
	
	/**
	 * Sets the parameters for analytic.
	 * 
	 * @param parameters
	 * @throws IllegalArgumentException
	 */
	void setParameters(Parameters parameters) throws IllegalArgumentException;
	
	/**
	 * Runs the analytic and returns the result.
	 * 
	 * @return
	 * @throws IOException
	 */
	Result getResult() throws IOException;
	
	/**
	 * Closes any connections created during the running of the analytic.
	 */
	void close();
	
}
