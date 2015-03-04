$(document).ready(function() {
	loadlist($('#DatasetName-xmd').get(0),/*the 'select' object*/
     		 'list?type=dataset',/*the url of the server-side script*/
     		 '_alias',/*The name of the field in the returned list*/
     		 'name'
     		 );

	var container = $('#jsoncontainer')[0];
	var options = {
		mode: 'form',
		change: jsonChange
	};
	var editor = new JSONEditor(container, options);

	var currentAlias = "";
	$("#submit-xmd-btn").prop('disabled', true);

	var firstTime = true;

	var json = {
   		
	};
	editor.set(json);
	editor.expandAll();

	$("button[name=getjson]").click(getJson);
	$("button[name=postjson]").click(sendJson);

	function getJson(event){
		if (($('#DatasetName-xmd').val()))
		{
			var emToGet = $('#DatasetName-xmd').val();
			full_url = "/json?type=xmd&datasetAlias=" + emToGet;
			$.getJSON(full_url, {}, function(data){
					cleanSystemFields(data);
					editor.set(data);
					editor.expandAll();
					currentAlias = emToGet;
					$("#submit-xmd-btn").prop('disabled', true);
					if (!firstTime){
						submittedButton();
					}
					firstTime = false;
			})
		}
	}

	function jsonChange(){
		$("#submit-xmd-btn").prop('disabled', false);

		if ($("submit-xmd-btn").hasClass("btn-success")){
			submittedButton();
		}
	}

	function sendJson(event){
		$.ajax({
		    url: '/json',
		    type: 'POST',
		    data: {	
		    			jsonString: JSON.stringify(editor.get()),
		    			type:'xmd',
		    			datasetAlias:currentAlias
		     		},
		    contentType: 'application/x-www-form-urlencoded; charset=utf-8',
		    dataType: 'json',
		    async: false,
		    success: function() {
		        submittedButton();
		    }
		});
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
	    });

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
	    });
	}


	function submittedButton(){
		$("#submit-xmd-btn").toggleClass("btn-danger");
		$("#submit-xmd-btn").toggleClass("btn-success");

		if ($("#submit-xmd-btn").hasClass("btn-danger")){
			$("#submit-xmd-btn").text("Submit Updated XMD");
		}
		else{
			$("#submit-xmd-btn").text("XMD Submitted!");
		}
	}

	$(document).ajaxSend(function(event, request, settings) {
		  $('#loading-indicator').show();
		});

	$(document).ajaxComplete(function(event, request, settings) {
			  $('#loading-indicator').hide();
	});
});


