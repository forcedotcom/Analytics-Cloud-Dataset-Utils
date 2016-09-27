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
    
    loadlistAndSelectize($('#DatasetName').get(0),/*the 'select' object*/
     		 'list?type=dataset',/*the url of the server-side script*/
     		 '_alias',/*The name of the field in the returned list*/
     		 'name'
     		 );

    loadlist($('select#DatasetApp').get(0),/*the 'select' object*/
    		 'list?type=folder',/*the url of the server-side script*/
    		 'developerName',/*The name of the field in the returned list*/
    		 'name'
    		 );
        
    $("#uploadForm").on('submit',(function(e) {
    	$("#title2").empty();
    	$("#result").empty();
        e.preventDefault();
        $.ajax({
            url: "upload",
            type: "POST",
            data:  new FormData(this),
            contentType: false,
            cache: false,
            processData:false,
            dataType:  'json',
            success: function(data){
            	self.location.href = 'logs.html';            	
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



$(document).on('change', '.btn-file :file', function() {
	  var input = $(this),
	      numFiles = input.get(0).files ? input.get(0).files.length : 1,
	      label = input.val().replace(/\\/g, '/').replace(/.*\//, '');	      
	  input.trigger('fileselect', [numFiles, label]);
	});

	$(document).ready( function() {
	    $('.btn-file :file').on('fileselect', function(event, numFiles, label) {
	        
	        var input = $(this).parents('.input-group').find(':text'),
	            log = numFiles > 1 ? numFiles + ' files selected' : label;
	        
	        if( input.length ) {
	            input.val(log);
	        } else {
	            if( log ) alert(log);
	        }	        
	        input.keyup();
	    });
	});

$(document).ajaxSend(function(event, request, settings) {
		  $('#loading-indicator').show();
		});

$(document).ajaxComplete(function(event, request, settings) {
		  $('#loading-indicator').hide();
});	


function deleteRow(tableID) {
    try {
        var table = document.getElementById(tableID);
        var rowCount = table.rows.length;
        for (var i = 0; i < rowCount; i++) {
            var row = table.rows[i];
             table.deleteRow(i);
             rowCount--;
             i--;
        }
    } catch (e) {
        alert(e);
    }
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
                        .text(obj[displayattr]));
        })
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
                        .text(obj[displayattr]));
        })        
    })
    .fail(function(jqXHR, textStatus, errorThrown) { 
        if (isEmpty(jqXHR.responseText) || jqXHR.responseText.indexOf("<!DOCTYPE HTML>") > -1) {
            self.location.href = 'login.html';
        }else
        {
			        	   handleError($("#title2").get(0),jqXHR.responseText);
        }
    })
    .always(function() {
        $(selobj).toggleClass("form-control demo-default");

    	$(selobj).selectize({
    		create: true
    	});
      });
}

function loadDiv(selobj,url,nameattr,displayattr)
{
    $(selobj).empty();
    $.getJSON(url,{},function(data)
    {
        $.each(data, function(i,obj)
        {
            $(selobj).append(
                 $('<div></div>')
                 		.attr('data-value',obj[nameattr])
                 		.attr('data-selectable','')
                 		.attr('class','option')
            			.text(obj[displayattr]));
        })
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

function isEmpty(str) {
    return (!str || 0 === str.length);
}