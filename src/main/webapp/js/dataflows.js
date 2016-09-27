$(document).ready(function() {
    listDataflows();
            
	$('#createButton').click(function() {
			self.location.href = 'datafloweditor.html?create=true'; 
	});
});


function listDataflows(){
    $.getJSON('list?type=dataflow',{},function(data){
    	if (typeof data !== 'undefined' && data.length > 0) {
        	printTable(data);
        	if(hasLocal(data))
        	{
        		$('#createButton').prop('disabled', false);
        	}else
        	{
        		$('#createButton').prop('disabled', true);	
        	}            
    	}else
    	{
     	   var tmp = $('<tr/>').append('').html("<td colspan=\"7\">No Dataflows found</td>");
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

function deleteDataflow(id){		
	var url = "list?type=dataflowDelete&dataflowAlias=" + id;
    $.getJSON(url,{},function(data){
    	$( "#"+id).remove();
    	var rowCount = $('#result tr').length;
    	if(rowCount == 5)
    	{
    		listDataflows();
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

function startDataflow(id){		
	var url = "list?type=dataflowStart&dataflowAlias=" + id;
    $.getJSON(url,{},function(data){
    	self.location.href = 'logs.html';            	
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


function copyDataflow(dataflowAlias,dataflowId){		
	var rowCount = $('#result tr').length;
   	if(rowCount == 5)
	{
		if (!confirm('Create a new local data flow? This will invalidate the dafault flow on the server')) { 
			return; 
		}		   
	}
	var url = "list?type=dataflowCopy&dataflowAlias=" + dataflowAlias + "&dataflowId="+dataflowId;
	$.getJSON(url,{},function(data){
    		listDataflows();
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
	    	var frequency = "n/a";
	    	if(data[i].refreshFrequencySec>0)
	    	{
	    		frequency = "Every " + (data[i].refreshFrequencySec/3600) + " Hour";
	    	}
	    	
	    	var runTime = "n/a";
	    	if(data[i].nextRunTime>0)
	    	{	    	
	    		runTime = new Date(data[i].nextRunTime).toLocaleString();
	    	}
    	   
        	var linkText = "<a href=\"datafloweditor.html?dataflowAlias="+data[i].name+"&dataflowId="+data[i]._uid+"\"><span class=\"name\">"+$('<div/>').text(data[i].masterLabel).html()+"</span></a>";

    	   var actions = " \
        	   <li><a href=\"#\" onclick='startDataflow(\""+data[i].name+"\");'>Start Now</a></li> \
        	   ";
        	   
	   	   if(data[i].status != "Active") 
    	   { 
    		   statusLabel = "<span class=\"label label-danger\">"+$('<div/>').text(data[i].status).html()+"</span>";

        	   actions = " \
            	   <li></li> \
            	   ";

    	   }else
    	   {
    	   	   statusLabel = "<span class=\"label label-success\">"+$('<div/>').text(data[i].status).html()+"</span>";

    	   }
    	       	       	  
    	   var tablerow =  " \
    	   <td class=\"hidden-phone\">"+linkText+"</td> \
    	   <td class=\"hidden-phone\"><span class=\"name\">"+$('<div/>').text(data[i]._lastModifiedBy.name).html()+"</span> </td> \
    	   <td class=\"hidden-phone\">"+$('<div/>').text(frequency).html()+"</td> \
    	   <td class=\"hidden-phone\">"+$('<div/>').text(runTime).html()+"</td> \
    	   <td class=\"hidden-phone\">"+$('<div/>').text(data[i].workflowType).html()+"</td> \
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
       	   tmp.attr("id",data[i].name);
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

function hasLocal(data) {
	var found = false;
    $.each(data, function(i,obj)
    {
    	if(data[i].name.indexOf("SalesEdgeEltWorkflowcopy") > -1)
    	{
    		found = true;
    	}
    });
    return found;
}
