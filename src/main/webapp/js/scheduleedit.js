$(document).ready(function() {	

     orderCount = 0;

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
 	
	var scheduleAlias = decodeURIComponent(urlParam('scheduleAlias'));
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
		
	if (scheduleAlias == undefined || isEmpty(scheduleAlias) ) 
	{
		if(create)
		{
			var selectedValues = [];
			loadlistAndSelectize($('select#jobs').get(0),/*the 'select' object*/
			    		 'list?type=dataflow',/*the url of the server-side script*/
			    		 'name',/*The name of the field in the returned list*/
			    		 'masterLabel',
			    		 selectedValues
			    		 );

			$("#scheduleAlias").prop("disabled", false);
			$("#scheduleAlias").change();
			$('#startDateTime').val(new Date((new Date()).getTime() + 5*60000).toLocaleString());
			$("#startDateTime").change();
		}else
		{
			self.location.href = 'dataflows.html';
		}
	}else
	{
		getSchedule(scheduleAlias);
	}

	   
	$("button[name=postjson]").click(sendJson);
	
	$('#frequency').change(function() {
	    if ($(this).val() === 'Hour') 
	    {
	    	$("#interval").empty();
	    	for(var i=1;i<13;i++)
	    	{
	            $('#interval').append(
		                 $('<option></option>')
		                        .val(i)
		                        .html(i));
		    }
	    	$('#interval').prop("disabled", false);
	//	    $('#interval').change();
	    } else if ($(this).val() === 'Minute') {
	    	$("#interval").empty();
	    	for(var i=5;i<31;i=i+5)
	    	{
	    		$('#interval').append(
	                 $('<option></option>')
	                        .val(i)
	                        .html(i));
	    	}
	    	$('#interval').prop("disabled", false);
//		    $('#interval').change();
	    } else {
	    	$("#interval").empty();
	    	for(var i=1;i<2;i++)
	    	{
	            $('#interval').append(
		                 $('<option></option>')
		                        .val(i)
		                        .html(i));
	    	}
//	    	$('#interval').change();
	    	$('#interval').prop("disabled", true);
		   }        
	});

	
	function sendJson(event){

		scheduleAlias = $("#scheduleAlias").val();
		if(isEmpty(scheduleAlias))
		{
			alert("You must enter a valid Schedule Name!");
			return;
		}

		var dt = $("#startDateTime").val();
		if(isEmpty(dt))
		{
			alert("You must enter a valid Start Date Time!");
			return;
		}
		var stDate = new Date(dt);
		var startDateTime = stDate.getTime();
		if(isNaN(startDateTime))
		{
			alert("You must enter a valid Start Date Time!");
			return;			
		}

		var frequency = $("#frequency").val();
		if(isEmpty(frequency) || frequency === 'none')
		{
			alert("You must enter a frequency!");
			return;
		}

		var interval = $("#interval").val();		
		if(isEmpty(interval) || interval === '0')
		{
			interval = 1;
		}
		
		var selectedJobs = $('#jobs option:selected');
		if(selectedJobs == undefined ||  selectedJobs.length == 0 )
		{
			alert("You must select one or more jobs!");
			return;
		}

        var selected = [];
        selectedJobs.each(function() {
            selected.push([$(this).val(), $(this).data('order')]);
        });

        selected.sort(function(a, b) {
            return a[1] - b[1];
        });
		
		var jobs = [];
		for (var i = 0; i < selected.length; i++) {
			jobs.push(selected[i][0]);
		}
		
		setTimeout(function() {
			$.ajax({
			    url: '/list',
			    type: 'POST',
			    data: {	
			    	type: 'schedule',
			    	scheduleAlias: scheduleAlias,
			    	startDateTime:startDateTime,
			    	frequency:frequency,
			    	interval: interval,
			    	jobs: jobs,
			    	create: create
			     },
			    contentType: 'application/x-www-form-urlencoded; charset=utf-8',
			    dataType: 'json',
			    async: true,
			    success: function() {
					$('#submit-xmd-btn').button('reset');
					self.location.href = 'schedules.html';
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

function loadlistAndSelectize(selobj,url,nameattr,displayattr,selectedValues)
{
    $.getJSON(url,{},function(data)
    {
        $(selobj).empty();
        $.each(data, function(i,obj)
        {
        	if(obj['status'] === 'Active')
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
    	$(selobj).attr("multiple",true);
//    	$(selobj).multiselect();
        $(selobj).multiselect({
        	numberDisplayed: 1,
            onChange: function(option, checked) {
                if (checked) {
                    orderCount++;
                    $(option).data('order', orderCount);
                }
                else {
                    $(option).data('order', '');
                }
            }
        });
    	if(selectedValues.length>0)
   		{
          $(selobj).multiselect('select',selectedValues,true);
          $(selobj).change();
   		}
      });
}


function getSchedule(scheduleAlias){
    $.getJSON('list?type=schedule&scheduleAlias='+scheduleAlias,{},function(data){
    	if (typeof data !== 'undefined') {
    		$("#scheduleAlias").val(data.devName);
    		$("#scheduleAlias").change();
    		$("#scheduleAlias").prop("disabled", true);
    		
    		$("#frequency").val(data.frequency);
    		$("#frequency").change();
    		
    		$("#interval").val(data.interval);
    		$("#interval").change();
    		
    		$("#startDateTime").val(new Date(data.scheduleStartDate).toLocaleString());
    		$("#startDateTime").change();
    		
    		var selectedValues = [];
    		for (var k in data.jobDataMap){
    		    if (data.jobDataMap.hasOwnProperty(k)) {
    		    	selectedValues.push(k);
    		    }
    		}
    		
			loadlistAndSelectize($('select#jobs').get(0),/*the 'select' object*/
		    		 'list?type=dataflow',/*the url of the server-side script*/
		    		 'name',/*The name of the field in the returned list*/
		    		 'masterLabel',
		    		 selectedValues
		    		 );
    	}else
    	{
     	   var tmp = $('<tr/>').append('').html("<td colspan=\"6\">Schedule not found</td>");
     	   $("#title2").append('').html("<h5 style='text-align:center'><i style='color:#FF0000'> schedule "+scheduleAlias+" not found</i></h5>");
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

