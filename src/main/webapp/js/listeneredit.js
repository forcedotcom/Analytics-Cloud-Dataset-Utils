$(document).ready(function() {	

	$('.input-group input[required], .input-group textarea[required], .input-group select[required]').on('keyup change', function() {
		var $form = $(this).closest('form'),
            $group = $(this).closest('.input-group'),
			$addon = $group.find('.input-group-addon'),
			$icon = $addon.find('span'),
			$cont = $(this).closest('.container'),
			state = false;
            
    	if (!$group.data('validate')) {
			state = $(this).val() ? true : false;
		}else if ($group.data('validate') == "email") {
			state = /^([a-zA-Z0-9_\.\-])+\@(([a-zA-Z0-9\-])+\.)+([a-zA-Z0-9]{2,4})+$/.test($(this).val())
		}else if($group.data('validate') == 'phone') {
			state = /^[(]{0,1}[0-9]{3}[)]{0,1}[-\s\.]{0,1}[0-9]{3}[-\s\.]{0,1}[0-9]{4}$/.test($(this).val())
		}else if ($group.data('validate') == "length") {
			state = $(this).val().length >= $group.data('length') ? true : false;
		}else if ($group.data('validate') == "number") {
			state = !isNaN(parseFloat($(this).val())) && isFinite($(this).val());
		}

		if (state) {
				$addon.removeClass('danger');
				$addon.addClass('success');
				$icon.attr('class', 'glyphicon glyphicon-ok');
		}else{
				$addon.removeClass('success');
				$addon.addClass('danger');
				$icon.attr('class', 'glyphicon glyphicon-remove');
		}
        
        if ($cont.find('.input-group-addon.danger').length == 0) {
            $('#submit-xmd-btn').prop('disabled', false);
            $('#submit-xmd-btn').removeClass('disabled')
        }else{
            $('#submit-xmd-btn').prop('disabled', true);
            $('#submit-xmd-btn').addClass('disabled');
        }
	});
    
    $('.input-group input[required], .input-group textarea[required], .input-group select[required]').trigger('change');
 	
	var listenerAlias = decodeURIComponent(urlParam('listenerAlias'));
	var create = decodeURIComponent(urlParam('create'));
	
	if (create == undefined || isEmpty(create) )
	{
		create = false;
	}else
	{
		if(create == 'true')
			create = true;
		else
			create = false;
	}
		
	if (listenerAlias == undefined || isEmpty(listenerAlias) ) 
	{
		if(create)
		{
			var selectedValues = null;
			loadlistAndSelectize($('#datasetAlias').get(0),/*the 'select' object*/
		     		 'list?type=dataset',/*the url of the server-side script*/
		     		 '_alias',/*The name of the field in the returned list*/
		     		 'name',
		     		selectedValues
		     		 );

		    loadlist($('select#datasetApp').get(0),/*the 'select' object*/
		    		 'list?type=folder',/*the url of the server-side script*/
		    		 'developerName',/*The name of the field in the returned list*/
		    		 'name',
		    		 selectedValues
		    		 );
		        
			$("#listenerAlias").prop("disabled", false);
			$("#listenerAlias").change();
		}else
		{
			self.location.href = 'listeners.html';
		}
	}else
	{
		getListener(listenerAlias);
	}

	   
	$("button[name=postjson]").click(sendJson);
	
	function sendJson(event){

		var listenerAlias = $("#listenerAlias").val();
		var datasetAlias = $("#datasetAlias").val();
		var datasetApp = $("#datasetApp").val();
		var operation = $("#operation").val();
		var inputFileDirectory = $("#inputFileDirectory").val();
		var inputFilePattern = $("#inputFilePattern").val();
		
		setTimeout(function() {
			$.ajax({
			    url: '/list',
			    type: 'POST',
			    data: {	
			    	type: 'listener',
			    	listenerType: 'file',
			    	listenerAlias: listenerAlias,
			    	datasetAlias:datasetAlias,
			    	datasetApp: datasetApp,
			    	operation: operation,
			    	inputFileDirectory: inputFileDirectory,
			    	inputFilePattern: inputFilePattern,
			    	create: create
			     },
			    contentType: 'application/x-www-form-urlencoded; charset=utf-8',
			    dataType: 'json',
			    async: true,
			    success: function() {
					$('#submit-xmd-btn').button('reset');
					self.location.href = 'listeners.html';
			    },
	            error: function(jqXHR, status, error) {
					$('#submit-xmd-btn').button('reset');
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

});

$(document).ajaxSend(function(event, request, settings) {
	  $('#loading-indicator').show();
});

$(document).ajaxComplete(function(event, request, settings) {
	$('#loading-indicator').hide();
});

function isEmpty(str) {
    return (!str || 0 === str.length || str === 'null');
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

function loadlist(selobj,url,nameattr,displayattr,selectedValue)
{
    $.getJSON(url,{},function(data)
    {
        $(selobj).empty();
    	if(!isEmpty(selectedValue))
    	{
            $(selobj).append(
                    $('<option></option>')
                           .val(selectedValue)
                           .html(selectedValue));    		
    	}

        $.each(data, function(i,obj)
        {
        	if(obj[nameattr] === selectedValue)
        	{
        		return true;
        	}else
        	{
        		$(selobj).append(
                 $('<option></option>')
                        .val(obj[nameattr])
                        .html(obj[displayattr]));
        	}
        })        
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

function loadlistAndSelectize(selobj,url,nameattr,displayattr,selectedValue)
{
    $.getJSON(url,{},function(data)
    {
        $(selobj).empty();
    	if(!isEmpty(selectedValue))
    	{
            $(selobj).append(
                    $('<option></option>')
                           .val(selectedValue)
                           .html(selectedValue));    		
    	}

        $.each(data, function(i,obj)
        {
        	if(obj[nameattr] === selectedValue)
        	{
        		return true;
        	}else
        	{
        		$(selobj).append(
                 $('<option></option>')
                        .val(obj[nameattr])
                        .html(obj[displayattr]));
        	}
        })        
    })
    .fail(function(jqXHR, textStatus, errorThrown) { 
        if (isEmpty(jqXHR.responseText) || jqXHR.responseText.indexOf("<!DOCTYPE HTML>") > -1) {
            self.location.href = 'login.html';
        }else
        {
        	var err = eval("(" + jqXHR.responseText + ")");
            $("#title2").append('').html("<h5 style='text-align:center'><i style='color:#FF0000'>"+err.statusMessage+"</i></h5>");
        }
    })
    .always(function() {

//    	if(!isEmpty(selectedValue))
//    	{
//    		$(selobj).val(selectedValue);
//    	}
    	
    	$(selobj).toggleClass("form-control demo-default");

    	$(selobj).selectize({
    		create: true
    	});
    	
		$(selobj).change();    		
    	
      });
}

function getListener(listenerAlias){
    $.getJSON('list?type=listener&listenerAlias='+listenerAlias,{},function(data){
    	if (typeof data !== 'undefined') {
    		$("#listenerAlias").val(data.devName);
    		$("#listenerAlias").change();
    		$("#listenerAlias").prop("disabled", true);
    		
    		
		    if (data.params.hasOwnProperty('operation')) {
	    		$("#operation").val(data.params['operation']);
	    		$("#operation").change();
		    }

		    if (data.params.hasOwnProperty('inputFileDirectory')) {
	    		$("#inputFileDirectory").val(data.params['inputFileDirectory']);
	    		$("#inputFileDirectory").change();
		    }
		    
		    if (data.params.hasOwnProperty('inputFilePattern')) {
	    		$("#inputFilePattern").val(data.params['inputFilePattern']);
	    		$("#inputFilePattern").change();
		    }
    		   
		    var datasetAlias = null;
		    if (data.params.hasOwnProperty('datasetAlias')) {
		    	datasetAlias = data.params['datasetAlias'];
		    }

		    var datasetApp = null;
		    if (data.params.hasOwnProperty('datasetApp')) {
		    	datasetApp = data.params['datasetApp'];
		    }

			loadlistAndSelectize($('#datasetAlias').get(0),/*the 'select' object*/
		     		 'list?type=dataset',/*the url of the server-side script*/
		     		 '_alias',/*The name of the field in the returned list*/
		     		 'name',
		     		 datasetAlias
		     		 );

		    loadlist($('select#datasetApp').get(0),/*the 'select' object*/
		    		 'list?type=folder',/*the url of the server-side script*/
		    		 'developerName',/*The name of the field in the returned list*/
		    		 'name',
		    		 datasetApp
		    		 );
    	}else
    	{
     	   var tmp = $('<tr/>').append('').html("<td colspan=\"6\">Listener not found</td>");
     	   $("#title2").append('').html("<h5 style='text-align:center'><i style='color:#FF0000'> listener "+listenerAlias+" not found</i></h5>");
    	}
    })
    .fail(function(jqXHR, textStatus, errorThrown) { 
        if (isEmpty(jqXHR.responseText) || jqXHR.responseText.indexOf("<!DOCTYPE HTML>") > -1) 
        {
            self.location.href = 'login.html';
        }else
        {
        	   var err = eval("(" + jqXHR.responseText + ")");
            	$("#title2").append('').html("<h5 style='text-align:center'><i style='color:#FF0000'>"+err.statusMessage+"</i></h5>");
        }
    });
  }

