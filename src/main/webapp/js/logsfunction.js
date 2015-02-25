$(document).ready(function() {
    	$("#result-header").empty();
    	$("#result-body").empty();
        $.getJSON('list?type=session',{},function(data)
        {
            	$("tr:has(td)").remove();
                $("#result-header").append(
                		$('<tr/>')
                  		.append($('<th/>').text('Name'))
                  		.append($('<th/>').text('Start Time'))
                  		.append($('<th/>').text('End Time'))
                  		.append($('<th/>').text('Status'))
                  		.append($('<th/>').text('Total Rows'))
                  		.append($('<th/>').text('Error Rows'))
                  		.append($('<th/>').text('Error Message'))
                  		.append($('<th/>').text('Download'))
                		)//end $("#uploaded-files").append()
        	 $.each(data, function(i,obj)
        	 {
            		var link = "<a href='upload?id="+data[i].id+"&type=errorCsv'>"+data[i].targetErrorCount+"</a>";
            		if(data[i].targetErrorCount == 0)
            			link = ""+data[i].targetErrorCount;

               		var actionString = "<a href='upload?id="+data[i].id+"&type=sessionLog'>"+"Log"+"</a>";               	 
            		if(data[i].params.METADATA_JSON)
            			actionString = actionString + "&nbsp;|&nbsp;" + "<a href='upload?id="+data[i].id+"&type=metadataJson'>"+"Json"+"</a>";
  
            		var status1 = data[i].status;
            		if(data[i].params.SERVER_STATUS)
            			status1 = data[i].status + "&nbsp;|&nbsp;" + data[i].params.SERVER_STATUS;
            		
            		$("#result-body").append(
                    		$('<tr/>')
                      		.append($('<td/>').text(data[i].name))
                      		.append($('<td/>').text(data[i].startTimeFormatted))
                      		.append($('<td/>').text(data[i].endTimeFormatted))
                      		.append($('<td/>').html(status1))
                      		.append($('<td/>').text(data[i].targetTotalRowCount))
                      		.append($('<td/>').html(link))
                      		.append($('<td/>').text(data[i].message))
                      		.append($('<td/>').html(actionString))
                    		)//end $("#uploaded-files").append()
             });
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
                        .html(obj[displayattr]));
        });
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
                        .html(obj[displayattr]));
        });

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
            			.html(obj[displayattr]));
        });
    });
}