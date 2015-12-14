/**
 * Copyright 2015 Crown Copyright
 * Copyright 2011-2015 The Apache Software Foundation
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
package gaffer.accumulo;

import gaffer.CloseableIterable;
import gaffer.accumulo.utils.Accumulo;
import gaffer.accumulo.utils.AccumuloConfig;
import gaffer.graph.TypeValue;
import gaffer.graph.wrappers.GraphElement;
import gaffer.graph.wrappers.GraphElementWithStatistics;

import java.io.IOException;

import gaffer.statistics.SetOfStatistics;
import jline.ConsoleReader;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;

/**
 * A simple REPL shell that allows an administrator with direct access to Accumulo
 * to see data as {@link GraphElement}s and {@link SetOfStatistics}.
 *
 * This class is a modified version of Accumulo's Shell class (org.apache.accumulo.shell.Shell).
 */
public class Shell {

	protected AccumuloBackedGraph graph;
	protected boolean exitShell = false;
	protected Connector connector;
	protected String tableName;
	protected CloseableIterable<GraphElementWithStatistics> retriever;

	public Shell(String accumuloPropertiesFile) throws Exception {
		// Connect to Accumulo and sanity check that the table exists
		AccumuloConfig accConf = new AccumuloConfig(accumuloPropertiesFile);
		connector = Accumulo.connect(accConf);
		tableName = accConf.getTable();
		String user = connector.whoami();
		Authorizations authorizations = connector.securityOperations().getUserAuthorizations(user);
		boolean tableExists = connector.tableOperations().exists(tableName);
		if (!tableExists) {
			throw new Exception("Table " + tableName + " does not exist.");
		}
		System.out.println("Connected as user " + user + " to table " + tableName + " with the following authorizations:\n" + authorizations);

		// Create AccumuloBackedGraph
		this.graph = new AccumuloBackedGraph(connector, tableName);
	}

	public void run () {
		registerShutdownHook();
		try {
			ConsoleReader reader = new ConsoleReader();
			String query;
			System.out.println("Enter an entity type and value separated by a bar, e.g. \"customer|A\".\n"
					+ "Type \"roll\" to toggle roll up over time and visibility on and off.\n"
					+ "Type \"entity\" to only see entities; type \"edge\" to only see edges; type \"both\" to see both entities and edges.\n"
					+ "Type q to quit.");
			while (true) {
				reader.setDefaultPrompt(getDefaultPrompt());
				query = reader.readLine();
				if (query == null) {
					return;
				} // user canceled
				try {
					this.processQuery(reader, query);
				} catch (Exception e) {
					System.err.println(e);
					this.exitShell = true;
				}
				if (this.exitShell) {
					return;
				}
			}
		} catch (IOException e) {
			System.err.println("Exception:" + e.getMessage());
		}
	}

	protected void processQuery(ConsoleReader reader, String query) throws Exception {
		if (query.equalsIgnoreCase("q") || query.equalsIgnoreCase("quit") || query.equalsIgnoreCase("exit")) {
			this.exitShell = true;
			return;
		}

		if (query.equalsIgnoreCase("roll")) {
			this.graph.rollUpOverTimeAndVisibility(!graph.isRollUpOverTimeAndVisibilityOn());
			System.out.println("Roll up over time and visibility is now " + (graph.isRollUpOverTimeAndVisibilityOn() ? "on" : "off") + ".");
			return;
		}

		if (query.equalsIgnoreCase("entity")) {
			this.graph.setReturnEntitiesOnly();
			System.out.println("Only returning entities.");
			return;
		}
		
		if (query.equalsIgnoreCase("edge")) {
			this.graph.setReturnEdgesOnly();
			System.out.println("Only returning edges.");
			return;
		}
		
		if (query.equalsIgnoreCase("both")) {
			this.graph.setReturnEntitiesAndEdges();
			System.out.println("Returning both entities and edges.");
			return;
		}
		
		String[] terms = query.split("\\|");
		if (terms.length != 2) {
			System.err.println("Your query had the wrong number of bars. Try again.");
			return;
		}
		
		// Create TypeValue from query and get all results
		TypeValue typeValue = new TypeValue(terms[0], terms[1]);
		retriever = this.graph.getGraphElementsWithStatistics(typeValue);
		printResults(reader);
	}
	
	/**
	 * Some of the code in the following is based on code in the printLines() method
	 * in <code>org.apache.accumulo.core.util.shell.Shell</code>.
	 * 
	 * @param reader
	 */
	protected void printResults(ConsoleReader reader) {
		int count = 0;
		try {
			int linesPrinted = 0;
			String prompt = "-- hit any key to continue or 'q' to quit --";
			int lastPromptLength = prompt.length();
			int termWidth = reader.getTermwidth();
			int maxLines = reader.getTermheight();

			String peek = null;
			for (GraphElementWithStatistics elementWithStatistics : retriever) {
				count++;

				String nextLine = elementWithStatistics.toString();
				if (nextLine == null)
					continue;
				for (String line : nextLine.split("\\n")) {
					if (peek != null) {
						reader.printString(peek);
						reader.printNewline();
						linesPrinted += peek.length() == 0 ? 0 : Math.ceil(peek.length() * 1.0 / termWidth);

						// check if displaying the next line would result in
						// scrolling off the screen
						if (linesPrinted + Math.ceil(lastPromptLength * 1.0 / termWidth) + Math.ceil(prompt.length() * 1.0 / termWidth)
								+ Math.ceil(line.length() * 1.0 / termWidth) > maxLines) {
							linesPrinted = 0;
							int numdashes = (termWidth - prompt.length()) / 2;
							String nextPrompt = repeat("-", numdashes) + prompt + repeat("-", numdashes);
							lastPromptLength = nextPrompt.length();
							reader.printString(nextPrompt);
							reader.flushConsole();
							if (Character.toUpperCase((char) reader.readVirtualKey()) == 'Q') {
								reader.printNewline();
								return;
							}
							reader.printNewline();
							termWidth = reader.getTermwidth();
							maxLines = reader.getTermheight();
						}
					}
					peek = line;
				}
			}
			retriever.close();
			if (peek != null) {
				reader.printString(peek);
				reader.printNewline();
			}
		} catch (IOException e) {
			System.err.println(e.getMessage());
		}

		if (count == 0) {
			System.out.println("No results");
		}
	}

	private static String repeat(String s, int c) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < c; i++) {
			sb.append(s);
		}
		return sb.toString();
	}

	private String getDefaultPrompt() {
		return connector.whoami() + "@" + connector.getInstance().getInstanceName() + " " + tableName + "> ";
	}

	/**
	 * Ensure the retriever is shut down when the process terminates.
	 */
	private void registerShutdownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				if (retriever != null) {
					System.out.println("Caught shutdown hook - closing retriever");
					retriever.close();
				} else {
					System.out.println("Caught shutdown hook - retriever is null so not closing it");
				}
			}
		});
	}

	/**
	 * Specify one argument which is the location of the Accumulo config file.
	 * 
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// Usage
		if (args.length != 1) {
			System.err.println("Usage: " + Shell.class.getName() + " <accumulo_properties_file>");
			return;
		}

		Shell shell = new Shell(args[0]);
		shell.run();
	}

}