$(document).ready(function() {    
	$('.input-group input[required], .input-group textarea[required], .input-group select[required]').on('keyup change', function() {
	validateState($(this).get(0));
	/*
		var $form = $(this).closest('form'),
            $group = $(this).closest('.input-group'),
			$addon = $group.find('.input-group-addon'),
			$icon = $addon.find('span'),
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
        
        if ($form.find('.input-group-addon.danger').length == 0) {
            $form.find('[type="submit"]').prop('disabled', false);
        }else{
            $form.find('[type="submit"]').prop('disabled', true);
        }
        
        */
	});
        
    
    $('.input-group input[required], .input-group textarea[required], .input-group select[required]').trigger('change');
    
	$('#AuthEndpoint').selectize({
		create : true
	});
	
	
	$("input[name='loginType']").change(function(){
	      if ($(this).val() === 'oauth') {

	            	$("#login").html('Login with Salesforce');

	            	$("#UserName").prop('required',false);
	            	$("#UserName").prop('disabled',true);
	            	validateState($("#UserName").get(0));
	            	$("#UserNameDiv").hide('');

	            	$("#password").prop('required',false);
	            	$("#password").prop('disabled',true);
	            	validateState($("#password").get(0));
	            	$("#PasswordDiv").hide('');

	    } else if ($(this).val() === 'std') {

	            	$("#login").html('Sign in');

	            	$("#UserName").prop('required',true);
	            	$("#UserName").prop('disabled',false);
	            	validateState($("#UserName").get(0));
	            	$("#UserNameDiv").show('');

	            	$("#password").prop('required',true);
	            	$("#password").prop('disabled',false);
	            	validateState($("#password").get(0));
	            	$("#PasswordDiv").show('');
	    }         	

	});
        
    $("#loginForm").on(
		'submit',
		(function(e) {
			$("#title2").empty();
			$("#result").empty();
			e.preventDefault();
			$.ajax({
				url : "login",
				type : "POST",
				data : new FormData(this),
				contentType : false,
				cache : false,
				processData : false,
				dataType : 'json',
				success : function(data) {
					if(!isEmpty(data.statusMessage))
					{					 
						self.location.href = data.statusMessage;
					}else{
						self.location.href = 'finder.html';
					}
				},
		           error: function(jqXHR, status, error) {
		               if (isEmpty(jqXHR.responseText) || jqXHR.responseText.indexOf("<!DOCTYPE HTML>") > -1) {
		                   self.location.href = 'login.html';
		               }else
		               {
			        	   handleError($("#title2").get(0),jqXHR.responseText);
		               }
		          }
			});
	}));
    
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


    function setFormForLoginType() {
        var loginTypes = document.getElementsByName("loginType");
        for (var i = 0; i < loginTypes.length; i++) {
            if (loginTypes[i].checked) {
                switchLoginTypeTo(loginTypes[i]);
                break;
            }
        }
    }

    function switchLoginTypeTo(typeElem) {
        typeElem.checked = true;

        switch (typeElem.id) {
            case "loginType_oauth":
            	$("#login").html('Login with Salesforce');
            	$("#UserName").prop('required',false);
            	$("#password").prop('required',false);
                break;
            default:
            	$("#login").html('Sign in');
            	$("#UserName").prop('required',true);
            	$("#password").prop('required',true);
                break;
        }
    }
    
    
    function validateState(selobj) {
		var $form = $(selobj).closest('form'),
            $group = $(selobj).closest('.input-group'),
			$addon = $group.find('.input-group-addon'),
			$icon = $addon.find('span'),
			state = false;
         
        if (!$(selobj).prop('required'))
        {   
         	state = true;   
    	}else if (!$group.data('validate')) {    		
			state = $(selobj).val() && $(selobj).prop('required') ? true : false;
		}else if ($group.data('validate') == "email") {
			state = /^([a-zA-Z0-9_\.\-])+\@(([a-zA-Z0-9\-])+\.)+([a-zA-Z0-9]{2,4})+$/.test($(selobj).val())
		}else if($group.data('validate') == 'phone') {
			state = /^[(]{0,1}[0-9]{3}[)]{0,1}[-\s\.]{0,1}[0-9]{3}[-\s\.]{0,1}[0-9]{4}$/.test($(selobj).val())
		}else if ($group.data('validate') == "length") {
			state = $(selobj).val().length >= $group.data('length') ? true : false;
		}else if ($group.data('validate') == "number") {
			state = !isNaN(parseFloat($(selobj).val())) && isFinite($(selobj).val());
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
        
        if ($form.find('.input-group-addon.danger').length == 0) {
            $form.find('[type="submit"]').prop('disabled', false);
        }else{
            $form.find('[type="submit"]').prop('disabled', true);
        }
	}
    

