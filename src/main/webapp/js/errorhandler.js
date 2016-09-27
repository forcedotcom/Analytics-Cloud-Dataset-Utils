	// JSON RegExp borrowed from jquery/src/core.js
	rvalidchars = /^[\],:{}\s]*$/;
	rvalidbraces = /(?:^|:|,)(?:\s*\[)+/g;
	rvalidescape = /\\(?:["\\\/bfnrt]|u[\da-fA-F]{4})/g;
	rvalidtokens = /"[^"\\\r\n]*"|true|false|null|-?(?:\d+\.|)\d+(?:[eE][+-]?\d+|)/g;

function handleError(errorDiv,errorMessage)
{
        if (isEmpty(errorMessage) || errorMessage.indexOf("<!DOCTYPE HTML>") > -1) {
            self.location.href = 'login.html';
        }else
        {
        	var errorShown = errorMessage;
		      $(errorDiv).empty();
        	   var err = parseJson(errorMessage);
        	   if ( err === null ) {
        			errorShown = errorMessage;
    			}else
    			{
    				errorShown = err.statusMessage;
    			}
            	$(errorDiv).append('').html("<h5 style='text-align:center'><i style='color:#FF0000'><div id='errorMsg'></div></i></h5>");
        		$("#errorMsg").text(errorShown);
        }
}

function isEmpty(str) {
    return (!str || 0 === str.length || str === 'null');
}

// Logic borrowed jquery parseJson, excepts this version does not throw an error
function parseJson(data) {
    if ( data === null ) {
        return data;
    }

    if ( typeof data === "string" ) {

        // Make sure leading/trailing whitespace is removed (IE can't handle it)
        data = jQuery.trim( data );

        if ( data ) {
            // Make sure the incoming data is actual JSON
            // Logic borrowed from http://json.org/json2.js
            if ( rvalidchars.test( data.replace( rvalidescape, "@" )
                .replace( rvalidtokens, "]" )
                .replace( rvalidbraces, "")) ) {

			    if ( window.JSON && window.JSON.parse ) {
			        return window.JSON.parse( data );
			    }
            }
        }
    }
	return null;
}