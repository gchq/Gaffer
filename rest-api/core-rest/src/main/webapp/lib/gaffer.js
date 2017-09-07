/*
 * Copyright 2016 Crown Copyright
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
function addExampleButtons(){

    var tables = $("#resource_operations .operation-params")

    for (var i = 0; i < tables.length; i++) {
        var table = tables[i]
        var rows = table.children
        for (var j = 0; j < rows.length; j++) {
            var row = rows[j]
            var firstCell = row.firstElementChild
            if (firstCell.firstElementChild.innerText == "body") {
                row.children[2].innerHTML = "<input type='button' value='Example JSON' onclick='if(loadExample){loadExample(this)}'>"
            }
        }
    }
}

function loadExample(exampleButton){

    var footer = document.getElementById("swagger-ui-container").children[1].children[2].innerText

    var version = footer.substr(footer.lastIndexOf(':') + 2, 2)

    var classPathParam = ""

    var operation = $(exampleButton).closest('.operation')

    var row = operation.find(".operation-params")[0].firstElementChild

    if (row.firstElementChild.textContent == "className") {
        classPathParam = row.children[1].firstElementChild.value
    }

    var urlSuffix = operation.find(".path").text().trim();
    var method = operation.find(".http_method").text().trim();

    if (classPathParam == "" && urlSuffix.includes('{')) {
        classPathParam = "uk.gov.gchq.gaffer.operation.OperationChain"
        row.children[1].firstElementChild.value = "uk.gov.gchq.gaffer.operation.OperationChain"
    }

    var exampleUrl = version + "/example" + urlSuffix.replace(/\{(.*?)\}/, classPathParam);

    if (method != 'post') {
        exampleUrl = exampleUrl + '/' + method
    }

    var onSuccess = function(response){
        var json=JSON.stringify(response, null,"   ");
        $(exampleButton.parentElement.parentElement).find("textarea").val(json);
    };
    $.ajax({url: exampleUrl, success: onSuccess});
}

function log() {
    if ('console' in window) {
      console.log.apply(console, arguments);
    }
}

function init(onSwaggerComplete){
      window.swaggerUi = new SwaggerUi({
        url: "latest/swagger.json",
        dom_id: "swagger-ui-container",
        supportedSubmitMethods: ['get', 'post', 'put', 'delete'],
        onComplete: function(swaggerApi, swaggerUi){
          log("Loaded swagger");
              $('pre code').each(function(i,e){hljs.highlightBlock(e)});
              addExampleButtons();
              if(onSwaggerComplete) {
                  onSwaggerComplete();
              }
        },
        onFailure: function(data) {
          log("Unable to load SwaggerUI");
        },
        docExpansion: "none",
        jsonEditor: false,
        defaultModelRendering: 'schema',
        showRequestHeaders: false,
        showOperationIds: false,
        docExpansion: "none",
        sorter: "alpha",
        apisSorter: "alpha",
        operationsSorter: "alpha"
      });

      window.swaggerUi.load();
}
