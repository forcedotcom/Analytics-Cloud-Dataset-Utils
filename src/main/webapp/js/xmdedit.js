$(document).ready(function() {
	loadlist($('#DatasetName-xmd').get(0),/*the 'select' object*/
     		 'list?type=dataset',/*the url of the server-side script*/
     		 '_alias',/*The name of the field in the returned list*/
     		 'name'
     		 );

	var container = $('#jsoncontainer')[0];
	var options = {
		mode: 'tree',
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
		$("#submit-xmd-btn").text("Submitting XMD...");
		disableButtons(true);
		setTimeout(function() {
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
			    	$("#submit-xmd-btn").text("Submit Updated XMD");
			    	disableButtons(false);
	           	   	$("#submit-xmd-btn").prop('disabled', true);
			        submittedButton();
			    },
	            error: function(xhr, status, error) {
	           	   $("#submit-xmd-btn").text("Submit Updated XMD");
	           	   disableButtons(false);
	           	   $("#submit-xmd-btn").prop('disabled', true);
	               if (isEmpty(jqXHR.responseText) || jqXHR.responseText.indexOf("<!DOCTYPE HTML>") > -1) {
	                   self.location.href = 'login.html';
	               }
	               else
	               {
		        	    var err = eval("(" + xhr.responseText + ")");
		            	$("#title2").append('').html("<h5 style='text-align:center'><i style='color:#FF0000'>"+err.statusMessage+"</i></h5>");
	               }
	          	 }
			});
		},13);
		
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
			editor.setMode('tree');
			editor.expandAll();
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


function isEmpty(str) {
    return (!str || 0 === str.length);
}