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
$("#resource_graph\\/doOperation .operation-params").find("td:eq(2)").append("<input type='button' value='Example JSON' onclick='if(loadExample){loadExample(this)}'>");
}

function loadExample(exampleButton){
   var urlSuffix = $(exampleButton).closest('.operation').find(".path").text().trim();
    var exampleUrl = window.location.origin + window.location.pathname + "v1/example" + urlSuffix;
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

function init(){
      window.swaggerUi = new SwaggerUi({
      url:"/example-rest/v1/api-docs/",
      dom_id:"swagger-ui-container",
      supportedSubmitMethods: ['get','post','put','delete'],
      onComplete: function(swaggerApi, swaggerUi){
        log("Loaded swagger");
        $('pre code').each(function(i,e){hljs.highlightBlock(e)});
        addExampleButtons();
      },
      onFailure: function(data){
      log("Unable to load SwaggerUI");
      },
      docExpansion: "none",
      sorter: "alpha",
      apisSorter: "alpha",
      operationsSorter: "alpha"
    });

    window.swaggerUi.load();
}
