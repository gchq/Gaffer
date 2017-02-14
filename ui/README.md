Copyright 2016 Crown Copyright

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


UI
============
This module contains a Gaffer read only UI prototype that connects to a Gaffer REST API.

Limitations:
- There are currently no error messages.
- There is no validation.
- Read only - no data can be added via the UI.


There are two options for building and then running it:

Option 1 - Deployable war file
==============================

If you wish to deploy the war file to a container of your choice, then use this option.

To build the war file along with all its dependencies then run the following command from the parent directory:
' mvn clean install -Pquick'

To deploy it to a server of your choice, take target/ui-[version].war and deploy as per the usual deployment process for your server.


Option 2 - Build using the standalone-ui profile
=============================================

The application can be built and then run as a basic executable standalone war file from Maven. When run in this format, the example rest api is also deployed.

To build it and its dependencies, use the following command from the parent directory:

'mvn clean install -Pquick -Pstandalone-ui'
This uses the 'standalone-ui' profile to start a tomcat server with the ui and example-rest wars deployed.

The ui can then be accessed via http://localhost:8080/ui and the rest api is at http://localhost:8080/rest

Adding data using the REST API via Swagger:
- Navigate to the rest api
- Expand 'graphdoOperation'
- Expand 'PUT /graph/doOperation/add/elements'.
- Click 'Example json' you will get some example json for adding some basic elements to the graph - you can then edit this json to create different example elements.
- Click 'Try it out!'

Doing a query in the UI:
- Navigate to the UI
- Ensure you are on the 'Graph' tab
- Add a seed
  - Click 'Add Seed'
  - Select a vertex type
  - Enter a vertex value (one that you have previously added e.g vertex1)
  - Click 'Add'.
- Build a query
  - Click 'Build query'
  - Select your seeds (or click 'Select all')
  - Click 'Next'
  - Select entity and edge groups using the checkboxes.
  - Click 'Next'
  - Add filters (optional)
  - Click 'Next'
  - Select any additional options required
  - Click 'Execute'