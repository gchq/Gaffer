Copyright 2022-2022 Crown Copyright

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Have A Go Operations
=============

Your FederatedStore  will be empty the first time, so a graph followed by elements will need to be added. Example operations can be found here.

## JSON
Example json operations can be found at `json-operation-examples`. 
Copy & paste the files contents into the Swaggers execute panel.

## Curl Command
Example curl commands within shell scripts can be found at `curl-opertion-examples`. 
Execute these shell scripts or copy & paste the curl command into your terminal.
Curl commands with the "alternative" in the name are duplicate examples that contain inline JSON
instead of reference to a .json file. This allows users to have a go at changing and editing operations in the command line.

```bash
cd curl-operation-examples/
./curl_getAllGraphIds.sh
```

The rest api will be deployed to localhost:8080/rest.
