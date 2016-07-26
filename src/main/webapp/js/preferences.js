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
 	
 	getPreferences();
 	
	$("button[name=postjson]").click(sendJson);
	
	function sendJson(event){

		var notificationLevel = $("#notificationLevel").val();
		var notificationEmail = $("#notificationEmail").val();
		var fiscalMonthOffset = $("#fiscalMonthOffset").val();
		var firstDayOfWeek = $("#firstDayOfWeek").val();
		var isYearEndFiscalYear = $("#isYearEndFiscalYear").val();
		
		setTimeout(function() {
			$.ajax({
			    url: '/list',
			    type: 'POST',
			    data: {	
			    	type: 'preferences',
			    	notificationLevel: notificationLevel,
			    	notificationEmail: notificationEmail,
			    	fiscalMonthOffset: fiscalMonthOffset,
			    	firstDayOfWeek: firstDayOfWeek,
			    	isYearEndFiscalYear: isYearEndFiscalYear
			     },
			    contentType: 'application/x-www-form-urlencoded; charset=utf-8',
			    dataType: 'json',
			    async: true,
			    success: function() {
					$('#submit-xmd-btn').button('reset');
					self.location.href = 'csvupload.html';
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


function getPreferences(){
   $.getJSON('list?type=preferences',{},function(data){
     	if (typeof data !== 'undefined') {
  
		    if (data.hasOwnProperty('notificationLevel')) {
	    		$("#notificationLevel").val(data['notificationLevel']);
	    		$("#notificationLevel").change();
		    }
		    
		    if (data.hasOwnProperty('notificationEmail')) {
	    		$("#notificationEmail").val(data['notificationEmail']);
	    		$("#notificationEmail").change();
		    }
    		   
		    if (data.hasOwnProperty('fiscalMonthOffset')) {
	    		$("#fiscalMonthOffset").val(data['fiscalMonthOffset']);
	    		$("#fiscalMonthOffset").change();
		    }

		    if (data.hasOwnProperty('firstDayOfWeek')) {
	    		$("#firstDayOfWeek").val(data['firstDayOfWeek']);
	    		$("#firstDayOfWeek").change();
		    }

		    if (data.hasOwnProperty('isYearEndFiscalYear')) {
	    		$("#isYearEndFiscalYear").val(data['isYearEndFiscalYear'].toString());
	    		$("#isYearEndFiscalYear").change();
		    }

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

