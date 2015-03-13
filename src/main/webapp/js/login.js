$(document).ready(function() {    
	$('.input-group input[required], .input-group textarea[required], .input-group select[required]').on('keyup change', function() {
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
	});
    
    $('.input-group input[required], .input-group textarea[required], .input-group select[required]').trigger('change');
    
	$('#AuthEndpoint').selectize({
		create : true
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
					self.location.href = 'csvupload.html';
				},
		           error: function(jqXHR, status, error) {
		               if (isEmpty(jqXHR.responseText) || jqXHR.responseText.indexOf("<!DOCTYPE HTML>") > -1) {
		                   self.location.href = 'login.html';
		               }else
		               {
			        	   var err = eval("(" + jqXHR.responseText + ")");
			            	$("#title2").append('').html("<h5 style='text-align:center'><i style='color:#FF0000'>"+err.statusMessage+"</i></h5>");
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