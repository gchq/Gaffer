function addExampleButtons(){
$("#resource_graph\\/doOperation .operation-params").find("td:eq(2)").append("<input type='button' value='Example JSON' onclick='if(loadExample){loadExample(this)}'>");
}

function loadExample(exampleButton){
    var urlSuffix = $(exampleButton.parentElement.parentElement.parentElement.parentElement.parentElement.parentElement.parentElement).find(".path").text().trim();
    // Bug in Gaffer since 0.3.3
    if ("/graph/doOperation" === urlSuffix){
        urlSuffix = "/graph/execute";
    }
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
