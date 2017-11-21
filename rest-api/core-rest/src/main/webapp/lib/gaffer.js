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

function getVersion() {
    var footer = document.getElementById("swagger-ui-container").children[1].children[2].innerText
    return footer.substr(footer.lastIndexOf(':') + 2, 2);
}

function addExampleButtons(){

    var tables = $("#resource_operations .operation-params")

    var exampleButton = "<input type='button' value='Add Example' onclick='loadExample(this)'>";
    var exampleOpsAndButton = "<select class='example-operations-select'><option value='uk.gov.gchq.gaffer.operation.OperationChain'>OperationChain</option></select>" + exampleButton;
    var exampleHtml;
    if(getVersion() === "v1") {
        exampleHtml = exampleButton;
    } else {
        exampleHtml = exampleOpsAndButton;
    }

    for (var i = 0; i < tables.length; i++) {
        var table = tables[i]
        var rows = table.children
        for (var j = 0; j < rows.length; j++) {
            var row = rows[j]
            var firstCell = row.firstElementChild
            if (firstCell.firstElementChild.innerText == "body") {
                row.children[2].innerHTML = exampleHtml;
            }
        }
    }

    initExampleOperations();
}

function loadExample(exampleButton){
    var version = getVersion();
    var exampleUrl;
    if(version === "v1") {
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

        exampleUrl = version + "/example" + urlSuffix.replace(/\{(.*?)\}/, classPathParam);
    } else {
        var classPathParam = $(exampleButton).parent().children(".example-operations-select").val();
        exampleUrl = version + "/graph/operations/" + classPathParam + "/example";
    }

    var onSuccess = function(response){
        var jsonStr = JSON.stringify(response, null,"   ");
        var textArea = $(exampleButton.parentElement.parentElement).find("textarea");
        var existingVal = textArea.val()
        if(existingVal !== '') {
            try {
                var existingJson = JSON.parse(existingVal);
                if(!existingJson.operations) {
                    existingJson = {
                       class: "uk.gov.gchq.gaffer.operation.OperationChain",
                       operations: [existingJson]
                    };
                }

                existingJson.operations.push(response);
                jsonStr = JSON.stringify(existingJson, null,"   ");
            } catch(e) {
              // just override the existing.
            }
        }
        textArea.val(jsonStr);
    };
    $.ajax({url: exampleUrl, success: onSuccess});
}

function log() {
    if ('console' in window) {
      console.log.apply(console, arguments);
    }
}

function initExampleOperations() {
    var availableOperationsSelect = $('.example-operations-select');
    $.get(
          getVersion() + '/graph/operations',
          null,
          function(availableOperations){
               // Sort operations based on name of operation (not including the package name)
               availableOperations.sort(function(a,b){return a.split('.').pop().localeCompare(b.split('.').pop())})
               if(availableOperationsSelect && availableOperationsSelect.size() > 0 && availableOperations) {
                  $.each(availableOperations,function(index, item) {
                      var opName = item.split('.').pop();
                      if("OperationChain" !== opName && "OperationChainDAO" !== opName) {
                        availableOperationsSelect.append('<option value=' + item + '>' + opName + '</option>');
                      }
                  });
               }
          }
     )
}

function init(onSwaggerComplete, onPropertiesLoad){
      window.swaggerUi = new SwaggerUi({
        url: "latest/swagger.json",
        dom_id: "swagger-ui-container",
        supportedSubmitMethods: ['get', 'post', 'put', 'delete'],
        onComplete: function(swaggerApi, swaggerUi){
          log("Loaded swagger");
              $('pre code').each(function(i,e){hljs.highlightBlock(e)});
              initFromProperties(onPropertiesLoad);
              addExampleButtons();
              hideJobsIfRequired();
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

function initFromProperties(onPropertiesLoad) {
    var onSuccess = function(properties) {
        updateTitle(properties);
        updateDescription(properties);
        updateBanner(properties);
        updateDocUrl(properties);
        if(onPropertiesLoad) {
            onPropertiesLoad(properties);
        }
    }
    $.get(getVersion() + '/properties', null, onSuccess);
}

function hideJobsIfRequired() {
    $.get(getVersion() + '/graph/operations/uk.gov.gchq.gaffer.operation.impl.job.GetJobDetails')
    .fail(function() {
         $("#resource_job").attr("hidden", true);
    })
}

function updateTitle(properties) {
    updateElement('gaffer.properties.app.title', properties, function(value, id) {
        $('#' + id).text(value);
        document.title = value;
    });
}

function updateBanner(properties) {
    if($('#banner').length == 0){
        updateElementWithId('banner', 'gaffer.properties.app.banner.description', properties, function (value, id) {
            $('body').prepend("<div id='banner' class='banner'>" + value + "</div>")
            updateElementWithId('banner', 'gaffer.properties.app.banner.colour', properties, function(value, id) {
                $('#' + id).css({'background-color': value});
            });
        });
    }
}

function updateDescription(properties) {
    updateElement('gaffer.properties.app.description', properties, function(value, id) {
        $('#' + id).html(value);
    });
}

function updateDocUrl(properties) {
    updateElementWithId('doc-url', 'gaffer.properties.app.doc.url', properties, function(value, id) {
        $('#' + id).html("For more information see our <a href='" + value + "'>documentation</a>.");
    });
}

function updateElement(key, properties, onSuccess) {
    updateElementWithId(key.split('.').pop(), key, properties, onSuccess);
}

function updateElementWithId(id, key, properties, onSuccess) {
    if(key in properties) {
        if(onSuccess) {
            var value = properties[key];
            if(value != null && value !== '') {
                onSuccess(value, id);
            }
        }
    }
}
