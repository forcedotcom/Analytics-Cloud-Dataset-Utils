$(document).ready(function() {
    var historyData = null;
    var currentData = null;
    var gettingHistory = false;

    refreshActive();
    refreshHistory();

    var showHistory = false;

    $("#hide-history").on("click", function(){
      $("#show-history").removeClass("active");
      $("#hide-history").addClass("active");
      showHistory = false;
      refreshActive();
    });

     $("#show-history").on("click", function(){
      $("#hide-history").removeClass("active");
      $("#show-history").addClass("active");
      showHistory = true;
      refreshActive();
      refreshHistory();
    });

    $("#hide-history").click();
    

    $("#result-header").empty();
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

    setInterval(function(){
    		refreshActive(); // this will run after every 5 seconds
    }, 5000);	

    setInterval(function(){
      refreshHistory();
    }, 45000);

    function selectButton(element){
      if (element == "hide"){
          $("hide-history").addClass("active");
          $("show-history").removeClass("active");
          gettingHistory = false;
          showHistory = false;
        }
      else{
          $("hide-history").removeClass("active");
          $("show-history").addClass("active");
          showHistory = true;
      }

    }

    function getTableData(type){
      $.getJSON('list?type='+type,{},function(data){
        if (type=="sessionHistory"){
          historyData = data;
          gettingHistory = false;
        }
        else{
          currentData = data;
        }
        displayOnTable(currentData, historyData);
      })
      .fail(function(jqXHR, textStatus, errorThrown) { 
	    	$("#title2").append('').html("<h5 style='text-align:center'><i style='color:#FF0000'>"+textStatus+"</i></h5>"); 
      });

    }

    function displayOnTable(current, history){
      $("#result-body").empty();

      if (current != null){
        printTable(current, "session");
      }
      if (history != null){
        printTable(history, "sessionHistory");
      }


      function printTable(data, type){
         $.each(data, function(i,obj)
         {
              var link = ""+data[i].targetErrorCount;
              
              if (data[i].status == "COMPLETED" && data[i].targetErrorCount != 0){
                
                var href_link = "<a href='upload?id="+data[i].id+"&type=errorCsv";
                if (type == "sessionHistory"){
                    href_link += "&history=true";
                }

                href_link += "'>";

                link = data[i].targetErrorCount+"&nbsp;&nbsp;&nbsp;"+href_link+"<span class=\"glyphicon glyphicon-th-list\"></span>"+"</a>";
              }

              var actionString = "<a href='upload?id="+data[i].id+"&type=sessionLog'>"+"Log"+"</a>";                 
              if(data[i].params.METADATA_JSON)
                actionString = actionString + "&nbsp;|&nbsp;" + "<a href='upload?id="+data[i].id+"&type=metadataJson'>"+"Json"+"</a>";

              if (type == "sessionHistory"){
                actionString = "&nbsp;";
              }

              var status1 = data[i].status;
              if(data[i].params.SERVER_STATUS)
                status1 = data[i].status + "&nbsp;|&nbsp;" + data[i].params.SERVER_STATUS;
              
              var tablerow = $('<tr/>');
              if (data[i].status == "ERROR" || data[i].params.SERVER_STATUS == "ERROR")
                tablerow.attr({'class': 'danger'});
              else if (data[i].targetErrorCount != 0 && data[i].status == "COMPLETED" && data[i].params.SERVER_STATUS == "COMPLETED")
                tablerow.attr({'class': 'warning'});
              else if (data[i].status == "COMPLETED" && data[i].params.SERVER_STATUS == "COMPLETED")
                tablerow.attr({'class': 'success'});

              $("#result-body").append(
                      tablerow
                        .append($('<td/>').text(data[i].name))
                        .append($('<td/>').text(data[i].startTimeFormatted))
                        .append($('<td/>').text(data[i].endTimeFormatted))
                        .append($('<td/>').html(status1))
                        .append($('<td/>').text(data[i].targetTotalRowCount))
                        .append($('<td/>').html(link))
                        .append($('<td/>').text(data[i].message))
                        .append($('<td/>').html(actionString))
                        );
            })
        } 
    }
    
    function refreshActive(){
    	if(!gettingHistory)
    		getTableData("session");
    }

    function refreshHistory(){
      if (showHistory){
        gettingHistory = true;
        getTableData("sessionHistory");
      }
    }
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
    })
    .fail(function(jqXHR, textStatus, errorThrown) { 
    	$("#title2").append('').html("<h5 style='text-align:center'><i style='color:#FF0000'>"+textStatus+"</i></h5>"); 
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
        })
        .fail(function(jqXHR, textStatus, errorThrown) { 
        	$("#title2").append('').html("<h5 style='text-align:center'><i style='color:#FF0000'>"+textStatus+"</i></h5>"); 
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