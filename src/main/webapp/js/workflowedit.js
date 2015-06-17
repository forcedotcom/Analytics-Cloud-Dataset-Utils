$(document).ready(function() {
	
	$("#submit-xmd-btn").prop('disabled', true);

	var container = $('#jsoncontainer')[0];
	var options = {
		mode: 'code',
		modes: ['code', 'tree'],
		change: jsonChange
	};
	var editor = new JSONEditor(container, options);

	var dataflowAlias = decodeURIComponent(urlParam('dataflowAlias'));
	if (dataflowAlias == undefined || isEmpty(dataflowAlias) ) 
	{
		self.location.href = 'dataflows.html';
	}
	
	var dataflowId = decodeURIComponent(urlParam('dataflowId'));

	getJson(editor,dataflowAlias,dataflowId);	

	$("button[name=postjson]").click(sendJson);


	function jsonChange()
	{
		$("#submit-xmd-btn").prop('disabled', false);
		if ($("#submit-xmd-btn").hasClass("btn-success")){
			submittedButton();
		}
	}

	function jsonError(err)
	{
		$("#submit-xmd-btn").prop('disabled', true);
		if ($("#submit-xmd-btn").hasClass("btn-success")){
			submittedButton();
		}
	}

	function sendJson(event){
		var json_string;

		try{
			json_string = JSON.stringify(editor.get());
		}
		catch(err){
			alert("Not a valid JSON!");
			return;
		}

		$("#submit-xmd-btn").text("Saving Dataflow...");
		disableButtons(true);
		setTimeout(function() {
			$.ajax({
			    url: '/json',
			    type: 'POST',
			    data: {	
			    			jsonString: json_string,
			    			type:'dataflow',
			    			dataflowAlias:dataflowAlias,
			    			dataflowId: dataflowId
			     		},
			    contentType: 'application/x-www-form-urlencoded; charset=utf-8',
			    dataType: 'json',
			    async: false,
			    success: function() {
			    	disableButtons(false);
	           	   	$("#submit-xmd-btn").prop('disabled', true);
			        submittedButton();
			    },
	            error: function(jqXHR, status, error) {
	           	   $("#submit-xmd-btn").text("Save Dataflow");
	           	   disableButtons(false);
	           	   $("#submit-xmd-btn").prop('disabled', true);
	               if (isEmpty(jqXHR.responseText) || jqXHR.responseText.indexOf("<!DOCTYPE HTML>") > -1) {
	                   self.location.href = 'login.html';
	               }
	               else
	               {
		        	    var err = eval("(" + jqXHR.responseText + ")");
		            	$("#title2").append('').html("<h5 style='text-align:center'><i style='color:#FF0000'>"+err.statusMessage+"</i></h5>");
	               }
	          	 }
			});
		},60);
		
	}

	function disableButtons(disEn){
		if(disEn){
			$("#submit-xmd-btn").prop('disabled', true);
			editor.setMode('view');
		}
		else{
			$("#submit-xmd-btn").prop('disabled', false);
			editor.setMode('code');
		}
	}


	function submittedButton(){
		$("#submit-xmd-btn").toggleClass("btn-danger");
		$("#submit-xmd-btn").toggleClass("btn-success");

		if ($("#submit-xmd-btn").hasClass("btn-danger")){
			$("#submit-xmd-btn").text("Save Dataflow");
		}
		else{
			$("#submit-xmd-btn").text("Dataflow Saved!");
		}
	}

});

$(document).ajaxSend(function(event, request, settings) {
	  $('#loading-indicator').show();
	});

$(document).ajaxComplete(function(event, request, settings) {
		  $('#loading-indicator').hide();
});

function isEmpty(str) {
    return (!str || 0 === str.length);
}

function urlParam(name){
    var results = new RegExp('[\?&]' + name + '=([^&#]*)').exec(window.location.href);
    if (results==null){
       return null;
    }
    else{
       return results[1] || 0;
    }
}

function getJson(editor,dataflowAlias,dataflowId){
		full_url = "/json?type=dataflow&dataflowAlias=" + dataflowAlias + "&dataflowId=" + dataflowId;
		$.getJSON(full_url, {}, function(data){
				editor.set(data);
		})
        .fail(function(jqXHR, textStatus, errorThrown) { 
            if (isEmpty(jqXHR.responseText) || jqXHR.responseText.indexOf("<!DOCTYPE HTML>") > -1) {
                self.location.href = 'login.html';
            }else
            {
	        	   var err = eval("(" + jqXHR.responseText + ")");
	            	$("#title2").append('').html("<h5 style='text-align:center'><i style='color:#FF0000'>"+err.statusMessage+"</i></h5>");
            }
        });
}