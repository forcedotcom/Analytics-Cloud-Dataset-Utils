$(document).ready(function() {
	
	loadlistStatic($('#DatasetName-xmd').get(0));

	var container = $('#jsoncontainer')[0];
	var options = {
		mode: 'code',
		modes: ['code', 'tree'],
		change: jsonChange
	};
	var editor = new JSONEditor(container, options);

	var currentAlias = "";
	$("#submit-xmd-btn").prop('disabled', true);

	var firstTime = true;

	var json = {
   		
	};
	editor.set(json);
	//editor.expandAll();

	$("button[name=getjson]").click(getJson);
	$("button[name=postjson]").click(sendJson);

	function getJson(event){
		if (($('#DatasetName-xmd').val()))
		{
			var emToGet = $('#DatasetName-xmd').val();
			full_url = "/json?type=dataflow&dataflowName=" + emToGet;
			$.getJSON(full_url, {}, function(data){
					cleanSystemFields(data);
					editor.set(data);
					//editor.expandAll();
					currentAlias = emToGet;
					$("#submit-xmd-btn").prop('disabled', true);
					if (!firstTime){
						submittedButton();
					}
					firstTime = false;
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
	}

	function jsonChange(){
		$("#submit-xmd-btn").prop('disabled', false);
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

		$("#submit-xmd-btn").text("Submitting Workflow...");
		disableButtons(true);
		setTimeout(function() {
			$.ajax({
			    url: '/json',
			    type: 'POST',
			    data: {	
			    			jsonString: json_string,
			    			type:'xmd',
			    			datasetAlias:currentAlias
			     		},
			    contentType: 'application/x-www-form-urlencoded; charset=utf-8',
			    dataType: 'json',
			    async: false,
			    success: function() {
			    	$("#submit-xmd-btn").text("Submit Updated Workflow");
			    	disableButtons(false);
	           	   	$("#submit-xmd-btn").prop('disabled', true);
			        submittedButton();
			    },
	            error: function(jqXHR, status, error) {
	           	   $("#submit-xmd-btn").text("Submit Updated Workflow");
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
			$("button[name=getjson]").prop('disabled', true);
			$('#DatasetName-xmd').prop('disabled', true);
			editor.setMode('view');
		}
		else{
			$("#submit-xmd-btn").prop('disabled', false);
			$("button[name=getjson]").prop('disabled', false);
			$('#DatasetName-xmd').prop('disabled', false);
			editor.setMode('code');
			//editor.expandAll();
		}
	}

	function cleanSystemFields(jsonObject){
		keyToDelete = [];
		for (var key in jsonObject) {
		    if (jsonObject.hasOwnProperty(key)) {
		    	console.log(key);
		    	console.log(key.charAt(0));
		        if (key.charAt(0) == "_"){
		        	console.log("delete");
		        	keyToDelete.push(key); 
		        }
		    }
		}

		$.each(keyToDelete, function(index, value){
			delete jsonObject[value];
		});

		return jsonObject;
	}

	function loadlistAndSelectize(selobj,url,nameattr,displayattr)
	{
	    $.getJSON(url,{},function(data)
	    {
	        $(selobj).empty();
	        $.each(data, function(i,obj)
	        {
	            $(selobj).append(
	                 $('<option></option>')
	                        .val(obj[nameattr])
	                        .html(obj[displayattr]));
	        });

	    	$(selobj).selectize({
	    		sortField: 'text'
	    	});
	    	$(".xmd-container").show();
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

	function loadlistStatic(selobj){
		$(selobj).empty();
		$(selobj).append(
	                 $('<option></option>')
	                        .val("SalesEdgeEltWorkflow")
	                        .html("SalesEdgeEltWorkflow"));
		$(".xmd-container").show();
	}
	
	function loadlist(selobj,url,nameattr,displayattr)
	{
	    $(selobj).empty();
	    $.getJSON(url,{},function(data)
	    {
	        $.each(data, function(i,obj)
	        {
	            $(selobj).append(
	                 $('<option></option>')
	                        .val(obj[nameattr])
	                        .html(obj[displayattr]));
	        });
	    	$(".xmd-container").show();
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


	function submittedButton(){
		$("#submit-xmd-btn").toggleClass("btn-danger");
		$("#submit-xmd-btn").toggleClass("btn-success");

		if ($("#submit-xmd-btn").hasClass("btn-danger")){
			$("#submit-xmd-btn").text("Submit Updated Workflow");
		}
		else{
			$("#submit-xmd-btn").text("Workflow Submitted!");
		}
	}

	$(document).ajaxSend(function(event, request, settings) {
		  $('#loading-indicator').show();
		});

	$(document).ajaxComplete(function(event, request, settings) {
			  $('#loading-indicator').hide();
	});
});


function isEmpty(str) {
    return (!str || 0 === str.length);
}