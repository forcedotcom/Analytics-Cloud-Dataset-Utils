$(document).ready(function() {
    listSchedules();
            
	$('#createButton').click(function() {
			self.location.href = 'scheduleeditor.html?create=true'; 
	});
});


function listSchedules(){
    $.getJSON('list?type=schedule',{},function(data){
    	if (typeof data !== 'undefined' && data.length > 0) {
        	printTable(data);
    	}else
    	{
     	   var tmp = $('<tr/>').append('').html("<td colspan=\"6\">No Schedules found</td>");
       	   tmp.attr("id","ErrorRow");
       	   tmp.addClass("alert-danger");
            $("#result-body").append(tmp)            
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

function deleteSchedule(id){		
	var url = "list?type=scheduleDelete&scheduleAlias=" + id;
    $.getJSON(url,{},function(data){
    	$( "#"+id).remove();
    	var rowCount = $('#result tr').length;
    	if(rowCount == 5)
    	{
    		listSchedules();
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

function enableSchedule(id){		
	var url = "list?type=scheduleEnable&scheduleAlias=" + id;
    $.getJSON(url,{},function(data){
    		listSchedules();
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

function disableSchedule(id){		
	var url = "list?type=scheduleDisable&scheduleAlias=" + id;
    $.getJSON(url,{},function(data){
    		listSchedules();
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


function printTable(data)
	{
	    $("#result-body").empty();
	    $.each(data, function(i,obj)
	    {
    	   var linkText = "<span class=\"name\">"+data[i].masterLabel+"</span>";
    	   if(data[i].disabled)
    	   {
    		   linkText = "<a href=\"scheduleeditor.html?scheduleAlias="+data[i].devName+"\"><span class=\"name\">"+data[i].masterLabel+"</span></a>";
    	   }

	    	var frequency = "n/a";
	    	if(data[i].interval>0)
	    	{
	    		frequency = "Every " + data[i].interval + " " + data[i].frequency;
	    	}
	    	
	    	var runTime = "n/a";
	    	if(data[i].nextRunTime>0)
	    	{	    	
	    		runTime = new Date(data[i].nextRunTime).toLocaleString();
	    	}
    	   
    	   var statusLabel = "<span class=\"label label-success\">Enabled</span>";
    	   var actions = " \
        	   <li><a href=\"#\" onclick='disableSchedule(\""+data[i].devName+"\");'>Disable</a></li> \
        	   ";
    	   if(data[i].disabled) 
    	   { 
    		   statusLabel = "<span class=\"label label-danger\">Disabled</span>";

        	   actions = " \
            	   <li><a href=\"#\" onclick='enableSchedule(\""+data[i].devName+"\");'>Enable</a></li> \
            	   <li><a href=\"#\" onclick='deleteSchedule(\""+data[i].devName+"\");'>Delete</a></li> \
            	   ";
    	   }
    	       	  
    	   var tablerow =  " \
    	   <td class=\"hidden-phone\">"+linkText+"</td> \
    	   <td class=\"hidden-phone\"><span class=\"name\">"+data[i].lastModifiedBy.name+"</span> </td> \
    	   <td class=\"hidden-phone\">"+frequency+"</td> \
    	   <td class=\"hidden-phone\">"+runTime+"</td> \
    	   <td class=\"hidden-phone\">"+statusLabel+"</td> \
    	   <td class=\"hidden-phone\"> \
    	   <div class=\"btn-group\"> \
    	   <button data-toggle=\"dropdown\" class=\"btn btn-xs dropdown-toggle\" data-original-title=\"\" title=\"\"> \
    	   Action \
    	   <span class=\"caret\"> \
    	   </span> \
    	   </button> \
    	   <ul class=\"dropdown-menu pull-right\">"+ actions +"</ul> \
    	   </div> \
    	   </td>"
    	   
    	   var tmp = $('<tr/>').append('').html(tablerow);
       	   tmp.attr("id",data[i].devName);
            $("#result-body").append(tmp);
          });
       $("#result-body").append($('<tr/>').attr('class', 'reset-this').append('').html("<td style=\"border-top : 0;\" colspan=\"6\">&nbsp;</td>"));
       $("#result-body").append($('<tr/>').attr('class', 'reset-this').append('').html("<td style=\"border-top : 0;\" colspan=\"6\">&nbsp;</td>"));
       $("#result-body").append($('<tr/>').attr('class', 'reset-this').append('').html("<td style=\"border-top : 0;\" colspan=\"6\">&nbsp;</td>"));

    }

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
