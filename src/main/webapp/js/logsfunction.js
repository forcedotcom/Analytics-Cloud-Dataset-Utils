$(document).ready(function() {
    var historyData = null;
    var currentData = null;

    refreshActive();
    
    setInterval(function(){
    		refreshActive(); // this will run after every 5 seconds
    }, 5000);	


    function getTableData(type){
      $.getJSON('list?type='+type,{},function(data){
        if (type=="sessionHistory"){
          historyData = data;
        }
        else{
          currentData = data;
        }
        displayOnTable(currentData, historyData);
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
              
              if ((data[i].status == "COMPLETED" || data[i].status == "FAILED" || data[i].status == "TERMINATED") && data[i].targetErrorCount != 0){
                
                var href_link = "<a href='upload?id="+data[i].id+"&type=errorCsv";
                if (type == "sessionHistory"){
                    href_link += "&history=true";
                }

                href_link += "'>";

                link = data[i].targetErrorCount+"&nbsp;&nbsp;&nbsp;"+href_link+"<span class=\"glyphicon glyphicon-download-alt\"></span>"+"</a>";
              }

              var actionString = "&nbsp;";
              if(data[i].type === "FileUpload")
              {
            	  actionString = "<a href='upload?id="+data[i].id+"&type=sessionLog'>"+"Log"+"</a>";                 
	              if(data[i].params.METADATA_JSON)
	                actionString = actionString + "&nbsp;|&nbsp;" + "<a href='upload?id="+data[i].id+"&type=metadataJson'>"+"Json"+"</a>";	
              }
              
              if (type == "sessionHistory"){
	                actionString = "&nbsp;";
              }
              
              var status1 = data[i].status;
              if(data[i].params.SERVER_STATUS)
                status1 = data[i].status + "&nbsp;|&nbsp;" + data[i].params.SERVER_STATUS;
              
              var tablerow = $('<tr/>');
              if (data[i].status == "FAILED" || data[i].params.SERVER_STATUS == "ERROR")
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
    		getTableData("session");
    }

});

$(document).ajaxSend(function(event, request, settings) {
		  $('#loading-indicator').show();
		});

$(document).ajaxComplete(function(event, request, settings) {
		  $('#loading-indicator').hide();
});

function isEmpty(str) {
    return (!str || 0 === str.length);
}