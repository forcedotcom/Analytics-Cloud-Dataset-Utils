$(document).ready(function() {

	$("#submit-xmd-btn").prop('disabled', true);

	var container = $('#jsoncontainer')[0];
	var options = {
		mode: 'code',
		modes: ['code', 'tree'],
		change: jsonChange
	};
	var editor = new JSONEditor(container, options);

	var datasetAlias = decodeURIComponent(urlParam('datasetAlias'));
	if (datasetAlias == undefined || isEmpty(datasetAlias) ) 
	{
		self.location.href = 'finder.html?current=false';
	}
	
	var datasetId = decodeURIComponent(urlParam('datasetId'));
	var datasetVersion = decodeURIComponent(urlParam('datasetVersion'));

	getJson(editor,datasetAlias,datasetId,datasetVersion);	

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

		$("#submit-xmd-btn").text("Submitting XMD...");
		disableButtons(true);
		setTimeout(function() {
			$.ajax({
			    url: '/json',
			    type: 'POST',
			    data: {	
			    			jsonString: json_string,
			    			type:'xmd',
			    			datasetAlias:datasetAlias,
			    			datasetId: datasetId,
			    			datasetVersion:datasetVersion			    			
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
	           	   $("#submit-xmd-btn").text("Submit XMD");
	           	   disableButtons(false);
	           	   $("#submit-xmd-btn").prop('disabled', true);
	               if (isEmpty(jqXHR.responseText) || jqXHR.responseText.indexOf("<!DOCTYPE HTML>") > -1) {
	                   self.location.href = 'login.html';
	               }
	               else
	               {
			        	   handleError($("#title2").get(0),jqXHR.responseText);
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
			$("#submit-xmd-btn").text("Submit XMD");
		}
		else{
			$("#submit-xmd-btn").text("XMD Submitted!");
		}
	}

});


$(document).ajaxSend(function(event, request, settings) {
	  $("#title2").empty();
	  $('#loading-indicator').show();
	});

$(document).ajaxComplete(function(event, request, settings) {
		  $('#loading-indicator').hide();
});


function cleanSystemFields(jsonObject){
	keyToDelete = [];
	for (var key in jsonObject) {
	    if (jsonObject.hasOwnProperty(key)) {
	        if (key.charAt(0) == "_"){
	        	keyToDelete.push(key); 
	        }
	    }
	}

	$.each(keyToDelete, function(index, value){
		delete jsonObject[value];
	});

	return jsonObject;
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


function isEmpty(str) {
    return (!str || 0 === str.length);
}

function getJson(editor,datasetAlias,datasetId,datasetVersion){
		full_url = "/json?type=xmd&datasetAlias=" + datasetAlias + "&datasetId=" + datasetId + "&datasetVersion=" + datasetVersion;
		$.getJSON(full_url, {}, function(data){
				cleanSystemFields(data);
				editor.set(data);
		})
        .fail(function(jqXHR, textStatus, errorThrown) { 
            if (isEmpty(jqXHR.responseText) || jqXHR.responseText.indexOf("<!DOCTYPE HTML>") > -1) {
                self.location.href = 'login.html';
            }else
            {
			        	   handleError($("#title2").get(0),jqXHR.responseText);
            }
        });
}
