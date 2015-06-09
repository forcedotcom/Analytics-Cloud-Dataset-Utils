$(document).ready(function() {        

});

// instantiate the uploader
Dropzone.options.fileupload = {
	    url: "/upload",
	    paramName: "inputCsv",
	    maxFiles: 1,
	    maxFilesize: 40000,
	    acceptedFiles: ".txt,.csv,.gz",
	    init: function() {	      
	      this.on('success', function(file, json) {
	    	  showPreview(file);
	      });		      	      
	    }
};


$(document).ajaxSend(function(event, request, settings) {
		  $('#loading-indicator').show();
		});

$(document).ajaxComplete(function(event, request, settings) {
		  $('#loading-indicator').hide();
});


function showPreview(file)
{
    var url = "csvpreview.html?type=file&name="+file.name;                 
	self.location.href = url;
}
