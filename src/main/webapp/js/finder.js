$(document).ready(function() {
    var historyData = null;
    var currentData = null;
    var gettingHistory = false;

	current = decodeURIComponent(urlParam('current'));

	if (current == undefined || isEmpty(current) )
	{
		current = false;
	}else
	{
		if(current == 'true')
			current = true;
		else
			current = false;
	}

    listDatasets(null);

	$('button[name=searchbtn]').click(searchDatasets);

	$('#searchinput').keypress(function (e) {
  		if (e.which == 13) {
    		$('button[name=searchbtn]').click();
    		return false;
  		}
	});

});

function searchDatasets(event){
		query = $("#searchinput").val();
		if(isEmpty(query))
		{
			listDatasets(null);
		}else
		{
			listDatasets(query);
		}
}

function listDatasets(query){
    $.getJSON('list?type=datasetAndApps&current='+current+'&search='+query,{},function(data){
    	if (typeof data !== 'undefined' && data.length > 0) {
    		if(data.length == 500)
    		{
	        	$("#header-count").text('Dataset Count: '+data.length + ' out of 500+');
	        }else
	        {
	        	$("#header-count").text('Dataset Count: '+data.length);
	        }
        	printTable(data);
    	}else
    	{
     	   var tmp = $('<tr/>').append('').html("<td colspan=\"6\">No Datasets found</td>");
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

function deleteDataset(datasetAlias,datasetId){
	var url = "list?type=deleteDataset&datasetAlias=" + datasetAlias + "&datasetId=" + datasetId;
    $.getJSON(url,{},function(data){
    	$( "#"+datasetAlias).remove();
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

    function printTable(data){
       $("#result-body").empty();
       $.each(data, function(i,obj)
       {
    	   var app = data[i].folder.label;
    	   if(isEmpty(app))
    	   {
    		   app = "Users Private App";
    	   }
    	   var buttonClass = "btn btn-xs dropdown-toggle";
    	   if(!data[i]._permissions.modify && !data[i]._permissions.manage)
    	   {
    		   buttonClass = buttonClass + " disabled";
    	   }

    	   var lastAccessed = "n/a";
	    	if(data[i]._lastAccessed>0)
	    	{
	    		lastAccessed = new Date(data[i]._lastAccessed).toLocaleString();
	    	}


    	   var tablerow =  "<td> \
    	   <a href=\"csvpreview.html?type=dataset&name="+data[i]._alias+ "&datasetId=" + data[i]._uid + "&datasetVersion=" + data[i].edgemartData._uid+"\"><span class=\"name\">"+$('<div/>').text(data[i].name).html()+"</span></a> \
    	   </td> \
    	   <td class=\"hidden-phone\">"+$('<div/>').text(app).html()+"</td> \
    	   <td> \
    	   <span class=\"name\">"+$('<div/>').text(data[i]._createdBy.name).html()+"</span> \
    	   </td> \
    	   <td class=\"hidden-phone\">"+new Date(data[i]._createdDateTime).toLocaleString()+"</td> \
    	   <td class=\"hidden-phone\">"+lastAccessed+"</td> \
    	   <td class=\"hidden-phone\"> \
    	   <div class=\"btn-group\"> \
    	   <button data-toggle=\"dropdown\" class=\""+buttonClass+"\" data-original-title=\"\" title=\"\"> \
    	   Action \
    	   <span class=\"caret\"> \
    	   </span> \
    	   </button> \
    	   <ul class=\"dropdown-menu pull-right\"> \
    	   <li> \
    	   <a href=\"list?datasetAlias=" + data[i]._alias + "&type=metadataJson"+"\">Download Metadata Json</a> \
    	   </li> \
    	   <li> \
    	   <a href=\"xmdeditor.html?datasetAlias=" + data[i]._alias + "&datasetId=" + data[i]._uid + "&datasetVersion=" + data[i].edgemartData._uid+"\">View Xmd</a> \
    	   </li> \
    	   <li> \
    	   <a href=\"#\" onclick='deleteDataset(\""+data[i]._alias+"\",\""+data[i]._uid+"\");'>Delete</a> \
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
    	  $("#title2").empty();
		  $('#loading-indicator').show();
		});

$(document).ajaxComplete(function(event, request, settings) {
		  $('#loading-indicator').hide();
});



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

function urlParam(name){
    var results = new RegExp('[\?&]' + name + '=([^&#]*)').exec(window.location.href);
    if (results==null){
       return null;
    }
    else{
       return results[1] || 0;
    }
}
