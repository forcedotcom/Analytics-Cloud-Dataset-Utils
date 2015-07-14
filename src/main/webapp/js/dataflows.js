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
        	   var err = eval("(" + jqXHR.responseText + ")");
            	$("#title2").append('').html("<h5 style='text-align:center'><i style='color:#FF0000'>"+err.statusMessage+"</i></h5>");
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
        	   var err = eval("(" + jqXHR.responseText + ")");
            	$("#title2").append('').html("<h5 style='text-align:center'><i style='color:#FF0000'>"+err.statusMessage+"</i></h5>");
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
        	   var err = eval("(" + jqXHR.responseText + ")");
            	$("#title2").append('').html("<h5 style='text-align:center'><i style='color:#FF0000'>"+err.statusMessage+"</i></h5>");
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
        	   var err = eval("(" + jqXHR.responseText + ")");
            	$("#title2").append('').html("<h5 style='text-align:center'><i style='color:#FF0000'>"+err.statusMessage+"</i></h5>");
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
    	   
    	   var statusLabel = "<span class=\"label label-success\">Active</span>";
    	   var linkText = "<a href=\"datafloweditor.html?dataflowAlias="+data[i].name+"&dataflowId="+data[i]._uid+"\"><span class=\"name\">"+data[i].masterLabel+"</span></a>";
    	   if(data[i].status != "Active") 
    	   { 
    		   statusLabel = "<span class=\"label label-danger\">Defunct</span>";
    		   linkText = "<span class=\"name\">"+data[i].masterLabel+"</span>";
    	   }
    	   
    	   var deleteText = "class=\"disabled\"";
    	   if(data[i].workflowType === 'Local')
    	   {
    		   deleteText = "";
    	   }
    	  
    	   var tablerow =  " \
    	   <td class=\"hidden-phone\">"+linkText+"</td> \
    	   <td class=\"hidden-phone\"><span class=\"name\">"+data[i]._lastModifiedBy.name+"</span> </td> \
    	   <td class=\"hidden-phone\">"+frequency+"</td> \
    	   <td class=\"hidden-phone\">"+runTime+"</td> \
    	   <td class=\"hidden-phone\">"+data[i].workflowType+"</td> \
    	   <td class=\"hidden-phone\">"+statusLabel+"</td> \
    	   <td class=\"hidden-phone\"> \
    	   <div class=\"btn-group\"> \
    	   <button data-toggle=\"dropdown\" class=\"btn btn-xs dropdown-toggle\" data-original-title=\"\" title=\"\"> \
    	   Action \
    	   <span class=\"caret\"> \
    	   </span> \
    	   </button> \
    	   <ul class=\"dropdown-menu pull-right\"> \
    	   <li>  \
    	   <a href=\"#\" onclick='startDataflow(\""+data[i].name+"\");'>Start Now</a> \
    	   </li> \
    	   <li "+ deleteText +">  \
    	   <a href=\"#\" onclick='deleteDataflow(\""+data[i].name+"\");'>Delete</a> \
    	   </li> \
    	   <li>  \
    	   <a href=\"#\" onclick='copyDataflow(\""+data[i].name+"\",\""+data[i]._uid+"\");'>Copy</a> \
    	   </li> \
    	   </ul> \
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
