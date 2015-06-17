$(document).ready(function() {
    listDataflows();
});


function listDataflows(){
    $.getJSON('list?type=dataflow',{},function(data){
    	if (typeof data !== 'undefined' && data.length > 0) {
        	printTable(data);
    	}else
    	{
     	   var tmp = $('<tr/>').append('').html("<td colspan=\"6\">No Dataflows found</td>");
       	   tmp.attr("id","ErrorRow");
       	   tmp.addClass("alert alert-danger");
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

    function printTable(data){
       $.each(data, function(i,obj)
       {
    	   var frequency = "Every " + (data[i].RefreshFrequencySec/3600) + " Hour";
    	  
    	   var tablerow =  "<td> \
    	   <a href=\"datafloweditor.html?dataflowAlias="+data[i].name+"&dataflowId="+data[i]._uid+"\"><span class=\"name\">"+data[i].MasterLabel+"</span></a> \
    	   </td> \
    	   <td> \
    	   <span class=\"name\">"+data[i]._lastModifiedBy.name+"</span> \
    	   </td> \
    	   <td class=\"hidden-phone\">"+frequency+"</td> \
    	   <td class=\"hidden-phone\">"+new Date(data[i].nextRunTime).toLocaleString()+"</td> \
    	   <td class=\"hidden-phone\"> \
    	   <span class=\"label label label-success\">Active</span> \
    	   </td> \
    	   <td class=\"hidden-phone\"> \
    	   <div class=\"btn-group\"> \
    	   <button data-toggle=\"dropdown\" class=\"btn btn-xs dropdown-toggle\" data-original-title=\"\" title=\"\"> \
    	   Action \
    	   <span class=\"caret\"> \
    	   </span> \
    	   </button> \
    	   <ul class=\"dropdown-menu pull-right\"> \
    	   <li class=\"disabled\"> \
    	   <a href=\"#\">Delete</a> \
    	   </li> \
    	   </ul> \
    	   </div> \
    	   </td>"
    	   
    	   var tmp = $('<tr/>').append('').html(tablerow);
       	   tmp.attr("id",data[i]._alias);
            $("#result-body").append(tmp);
          });
       $("#result-body").append($('<tr/>').attr('class', 'reset-this').append('').html("<td style=\"border-top : 0;\" colspan=\"6\">&nbsp;</td>"));
       $("#result-body").append($('<tr/>').attr('class', 'reset-this').append('').html("<td style=\"border-top : 0;\" colspan=\"6\">&nbsp;</td>"));
       $("#result-body").append($('<tr/>').attr('class', 'reset-this').append('').html("<td style=\"border-top : 0;\" colspan=\"6\">&nbsp;</td>"));

    }
  

$(document).ajaxSend(function(event, request, settings) {
		  $('#loading-indicator').show();
		});

$(document).ajaxComplete(function(event, request, settings) {
		  $('#loading-indicator').hide();
});


function isEmpty(str) {
    return (!str || 0 === str.length);
}