$(document).ready(function() {

var qs = (function(a) {
    if (a == "") return {};
    var b = {};
    for (var i = 0; i < a.length; ++i)
    {
        var p=a[i].split('=', 2);
        if (p.length == 1)
            b[p[0]] = "";
        else
            b[p[0]] = decodeURIComponent(p[1].replace(/\+/g, " "));
    }
    return b;
})(window.location.hash.substr(1).split('&'));

if(qs == null || !qs.hasOwnProperty('access_token'))
{

	qs = (function(a) {
	    if (a == "") return {};
	    var b = {};
	    for (var i = 0; i < a.length; ++i)
	    {
	        var p=a[i].split('=', 2);
	        if (p.length == 1)
	            b[p[0]] = "";
	        else
	            b[p[0]] = decodeURIComponent(p[1].replace(/\+/g, " "));
	    }
	    return b;
	})(window.location.search.substr(1).split('&'));
	
	if(qs == null || !qs.hasOwnProperty('error'))
	{
		self.location.href = 'login.html';	
	}else
	{
		var err = qs["error"] + ":" + qs["error_description"];
		if(isEmpty(err))
		{
			err = "OAUTH Flow failed";
		}
		$("#title2").append('').html("<h5 style='text-align:center'><i style='color:#FF0000'>"+err+"</i></h5>");
	}
}else
{
 			$("#title2").empty();
			$.ajax({
				url : "login",
				type : "POST",
				data : qs,
				cache : false,
				dataType : 'json',
				success : function(data) {
						self.location.href = 'finder.html';
				},
		           error: function(jqXHR, status, error) {
		               if (isEmpty(jqXHR.responseText) || jqXHR.responseText.indexOf("<!DOCTYPE HTML>") > -1) {
		                   self.location.href = 'login.html';
		               }else
		               {
			        	   var err = eval("(" + jqXHR.responseText + ")");
			            	$("#title2").append('').html("<h5 style='text-align:center'><i style='color:#FF0000'>"+err.statusMessage+"</i></h5>");
		               }
		          }
			}); 
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

