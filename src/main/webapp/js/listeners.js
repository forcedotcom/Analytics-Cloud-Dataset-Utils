$(document).ready(function() {
    listListeners();
            
	$('#createButton').click(function() {
			self.location.href = 'listenereditor.html?create=true'; 
	});
});


function listListeners(){
    $.getJSON('list?type=listener',{},function(data){
    	if (typeof data !== 'undefined' && data.length > 0) {
        	printTable(data);
    	}else
    	{
     	   var tmp = $('<tr/>').append('').html("<td colspan=\"6\">No Listeners found</td>");
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

function deleteListener(id){		
	var url = "list?type=listenerDelete&listenerAlias=" + id;
    $.getJSON(url,{},function(data){
    	$( "#"+id).remove();
    	var rowCount = $('#result tr').length;
    	if(rowCount <= 4)
    	{
    		listListeners();
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

function enableListener(id){		
	var url = "list?type=listenerEnable&listenerAlias=" + id;
    $.getJSON(url,{},function(data){
    	listListeners();
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

function disableListener(id){		
	var url = "list?type=listenerDisable&listenerAlias=" + id;
    $.getJSON(url,{},function(data){
    	listListeners();
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
	    	var uri = "";
		    if (data[i].params.hasOwnProperty('inputFileDirectory')) {
		    	uri = data[i].params['inputFileDirectory'];
		    }

		    if (data[i].params.hasOwnProperty('inputFilePattern')) {
		    	uri = uri + "(" + data[i].params['inputFilePattern'] + ")";
		    }else
		    {
		    	uri = uri + "(" + "*.csv" + ")";
		    }

		    var linkText = "<span class=\"name\">"+data[i].masterLabel+"</span>";
    	   if(data[i].disabled)
    	   {
    		   linkText = "<a href=\"listenereditor.html?listenerAlias="+data[i].devName+"\"><span class=\"name\">"+data[i].masterLabel+"</span></a>";
    	   }

    	   
    	   var statusLabel = "<span class=\"label label-success\">Enabled</span>";
    	   var actions = " \
        	   <li><a href=\"#\" onclick='disableListener(\""+data[i].devName+"\");'>Disable</a></li> \
        	   ";
    	   if(data[i].disabled) 
    	   { 
    		   statusLabel = "<span class=\"label label-danger\">Disabled</span>";

        	   actions = " \
            	   <li><a href=\"#\" onclick='enableListener(\""+data[i].devName+"\");'>Enable</a></li> \
            	   <li><a href=\"#\" onclick='deleteListener(\""+data[i].devName+"\");'>Delete</a></li> \
            	   ";
    	   }
    	       	  
    	   var tablerow =  " \
    	   <td class=\"hidden-phone\">"+linkText+"</td> \
    	   <td class=\"hidden-phone\"><span class=\"name\">"+uri+"</span> </td> \
    	   <td class=\"hidden-phone\"><span class=\"name\">"+data[i].lastModifiedBy.name+"</span> </td> \
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
