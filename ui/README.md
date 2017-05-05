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


If you wish to deploy the war file to a container of your choice, then use this option.

To build the war file along with all its dependencies then run the following command from the parent directory:
' mvn clean install -Pquick'

To deploy it to a server of your choice, take target/ui-[version].war and deploy as per the usual deployment process for your server.

There is a settings page in the ui where you can temporarily change the REST API URL, alternatively you will need to edit the settings.js file to set the URL permanently.

For an example of using the UI see the example/road-traffic module.