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
 	
 	getSettings();
 	
	$("button[name=postjson]").click(sendJson);
	
	function sendJson(event){

		var proxyUsername = $("#proxyUsername").val();
		var proxyPassword = $("#proxyPassword").val();
		var proxyNtlmDomain = $("#proxyNtlmDomain").val();
		var proxyHost = $("#proxyHost").val();
		var proxyPort = $("#proxyPort").val();
		var timeoutSecs = $("#timeoutSecs").val();
		var connectionTimeoutSecs = $("#connectionTimeoutSecs").val();
		var noCompression = false;
		if ($("#noCompression").is(":checked"))
		{
		  noCompression = true;
		}
		var debugMessages = false;
		if ($("#debugMessages").is(":checked"))
		{
		  debugMessages = true;
		}
		
		setTimeout(function() {
			$.ajax({
			    url: '/settings',
			    type: 'POST',
			    data: {	
			    	type: 'settings',
			    	proxyUsername: proxyUsername,
			    	proxyPassword: proxyPassword,
			    	proxyNtlmDomain: proxyNtlmDomain,
			    	proxyHost: proxyHost,
			    	proxyPort: proxyPort,
			    	timeoutSecs: timeoutSecs,
			    	connectionTimeoutSecs: connectionTimeoutSecs,
			    	noCompression: noCompression,
			    	debugMessages: debugMessages
			     },
			    contentType: 'application/x-www-form-urlencoded; charset=utf-8',
			    dataType: 'json',
			    async: true,
			    success: function() {
					$('#submit-xmd-btn').button('reset');
					self.location.href = 'login.html';
			    },
	            error: function(jqXHR, status, error) {
					$('#submit-xmd-btn').button('reset');
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

});

$(document).ajaxSend(function(event, request, settings) {
	  	$("#title2").empty();
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


function getSettings(){
    $.getJSON('settings',{},function(data){
    	if (typeof data !== 'undefined') {
  
		    if (data.hasOwnProperty('proxyUsername')) {
	    		$("#proxyUsername").val(data['proxyUsername']);
	    		$("#proxyUsername").change();
		    }
		    
		    if (data.hasOwnProperty('proxyPassword')) {
	    		$("#proxyPassword").val(data['proxyPassword']);
	    		$("#proxyPassword").change();
		    }
    		   
		    if (data.hasOwnProperty('proxyNtlmDomain')) {
	    		$("#proxyNtlmDomain").val(data['proxyNtlmDomain']);
	    		$("#proxyNtlmDomain").change();
		    }

		    if (data.hasOwnProperty('proxyHost')) {
	    		$("#proxyHost").val(data['proxyHost']);
	    		$("#proxyHost").change();
		    }

		    if (data.hasOwnProperty('proxyPort')) {
	    		$("#proxyPort").val(data['proxyPort']);
	    		$("#proxyPort").change();
		    }

		    if (data.hasOwnProperty('timeoutSecs')) {
	    		$("#timeoutSecs").val(data['timeoutSecs']);
	    		$("#timeoutSecs").change();
		    }

		    if (data.hasOwnProperty('connectionTimeoutSecs')) {
	    		$("#connectionTimeoutSecs").val(data['connectionTimeoutSecs']);
	    		$("#connectionTimeoutSecs").change();
		    }

		    if (data.hasOwnProperty('noCompression')) {
		    	if(data['noCompression'])
		    	{
		    		$('#noCompression').prop('checked', true);
		    		$("#noCompression").change();
	    		}
		    }

		    if (data.hasOwnProperty('debugMessages')) {
		    	if(data['debugMessages'])
		    	{
		    		$('#debugMessages').prop('checked', true);
		    		$("#debugMessages").change();
	    		}
		    }

    	}else
    	{
     	   var tmp = $('<tr/>').append('').html("<td colspan=\"6\">Settings not found</td>");
    	}
    })
    .fail(function(jqXHR, textStatus, errorThrown) { 
        if (isEmpty(jqXHR.responseText) || jqXHR.responseText.indexOf("<!DOCTYPE HTML>") > -1) 
        {
            self.location.href = 'login.html';
        }else
        {
 			        	   handleError($("#title2").get(0),jqXHR.responseText);
       }
    });
  }

