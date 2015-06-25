$(document).ready(function() {        

//	$('#saqlContainer').hide();
//	$('#saqlButton').prop('disabled', true);
//	$('#uploadButton').prop('disabled', true);
//	$('#augmentButton').prop('disabled', true);

	$('#exportButton').click(exportToCsv);

	
	var type = decodeURIComponent(urlParam('type'));
	if (type == undefined || isEmpty(type) ) 
	{
		self.location.href = 'csvupload2.html';
	}
		
	var name = decodeURIComponent(urlParam('name'));
	if (name == undefined || isEmpty(name) ) 
	{
		self.location.href = 'csvupload2.html';
	}

	if(type == 'dataset')
	{
		  $('#uploadButton').remove();	
	}
	
	if(type == 'file')
	{
		  $('#saqlButton').remove();	
	}
	
	$('#saqlModal').draggable({
	    handle: ".modal-header"
	});

	preview(type,name);	
});


$(document).ajaxSend(function(event, request, settings) {
		  $('#loading-indicator').show();
		});

$(document).ajaxComplete(function(event, request, settings) {
		  $('#loading-indicator').hide();
});

$(document).on("click", "#queryButton", function(event){
	$("#modal-title2").empty();
	$("#modal-title2").removeClass("alert alert-danger");
	sendSaql(event);
});

function uploadFile()
{
	var file = decodeURIComponent(urlParam('file'));
	if (file == undefined || isEmpty(file) ) 
	{
		self.location.href = 'csvupload2.html';
	}else
	{
		self.location.href = "csvupload.html?preview=true&file=" + file;		
	}
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


function sendSaql(event){
	var request_type = decodeURIComponent(urlParam('type'));
	var request_name = decodeURIComponent(urlParam('name'));
	var saql_string = $('#queryText').val();
	if(isEmpty(saql_string))
	{
		alert("Invalid SAQL!");
		return;
	}

    var buttonDomElement = event.target;
    $(buttonDomElement).attr('disabled', true);

//	$('#queryButton').prop('disabled', true);
		
	setTimeout(function() {
		$.ajax({
		    url: '/preview',
		    type: 'POST',
		    data: {	
		    			saql: saql_string,
		    			type: request_type,
		    			name:request_name
		     		},
		    contentType: 'application/x-www-form-urlencoded; charset=utf-8',
		    dataType: 'json',
		    async: true,
		    success: function(data) {
				var columns = data.columns;
				var pdata = data.data
				buildGrid(columns,pdata);
				$('#queryText').val(data.saql);
                $(buttonDomElement).attr('disabled', false);
				 $('#saqlModal').modal('hide');
		    },
            error: function(jqXHR, status, error) {
                $(buttonDomElement).attr('disabled', false);
               if (isEmpty(jqXHR.responseText) || jqXHR.responseText.indexOf("<!DOCTYPE HTML>") > -1) {
                   self.location.href = 'login.html';
               }
               else
               {
	        	    var err = eval("(" + jqXHR.responseText + ")");
	        	    $("#modal-title2").addClass("alert alert-danger");
	              	$("#modal-title2").append('').html("<h4 style='text-align:center'>"+err.statusMessage+"</h4>");
	              	if(err.statusData!=null && err.statusData.hasOwnProperty('saql'))
	              	{
	              		$('#queryText').val(err.statusData.saql);
	              	}
               }
          	 }
		});
	},60);
	
}


function preview(type,name)
{
	full_url = "/preview?type="+type+"&name="+name;
	$.getJSON(full_url, {}, function(data) {
				var columns = data.columns;
				var pdata = data.data
				buildGrid(columns,pdata);
				$('#queryText').val(data.saql);
				/*
					full_url = "/preview?type=filedata&file=" + file;
					$.getJSON(full_url, {}, function(data){
							buildGrid(columns,pdata);
					})
			        .fail(function(jqXHR, textStatus, errorThrown) { 
			            if (isEmpty(jqXHR.responseText) || jqXHR.responseText.indexOf("<!DOCTYPE HTML>") > -1) {
			                self.location.href = 'login.html';
			            }else
			            {
				        	   var err = eval("(" + jqXHR.responseText + ")");
				            	$('#status-label').text(err.statusMessage);
			            }
			        });
		        */					
		})
        .fail(function(jqXHR, textStatus, errorThrown) { 
            if (isEmpty(jqXHR.responseText) || jqXHR.responseText.indexOf("<!DOCTYPE HTML>") > -1) {
                self.location.href = 'login.html';
            }else
            {
	        	   var err = eval("(" + jqXHR.responseText + ")");
	        	    $("#title2").addClass("alert alert-danger");
	              	$("#title2").append('').html("<h4 style='text-align:center'>"+err.statusMessage+"</h4>");
	              	if(err.statusData!=null && err.statusData.hasOwnProperty('saql'))
	              	{
	              		$('#queryText').val(err.statusData.saql);
	              	}
	        }
        });
}

function buildGrid(columns,data) {
	
	var options = {
	        enableCellNavigation: true,
	        enableColumnReorder: false,
	        explicitInitialization: true,
	        editable: true
	    };	

    
	var dataView = new Slick.Data.DataView();
    
    var grid = new Slick.Grid("#grid", dataView, columns, options);
    
    grid.setSelectionModel(new Slick.CellSelectionModel());

    dataView.onRowCountChanged.subscribe(function (e, args) {
        grid.updateRowCount();
        grid.render();
    });

    dataView.onRowsChanged.subscribe(function (e, args) {
        grid.invalidateRows(args.rows);
        grid.render();
    });

    dataView.beginUpdate();
    dataView.setItems(data,"_id");
    dataView.setFilter(filter);
    dataView.endUpdate();

    var filterPlugin = new Ext.Plugins.HeaderFilter({});

    filterPlugin.onFilterApplied.subscribe(function () {
        dataView.refresh();
        grid.resetActiveCell();

        var status;

        if (dataView.getLength() === dataView.getItems().length) {
            status = "";
        } else {
            status = dataView.getLength() + ' OF ' + dataView.getItems().length + ' RECORDS FOUND';
        }
        $('#status-label').text(status);
    });

    filterPlugin.onCommand.subscribe(function (e, args) {
        var comparer = function (a, b) {
            return a[args.column.field] > b[args.column.field];
        };

        switch (args.command) {
            case "sort-asc":
                dataView.sort(comparer, true);
                break;
            case "sort-desc":
                dataView.sort(comparer, false);
                break;
        }
    });

    grid.registerPlugin(filterPlugin);

    var overlayPlugin = new Ext.Plugins.Overlays({ decoratorWidth: 1});

    overlayPlugin.onFillUpDown.subscribe(function (e, args) {
        var column = grid.getColumns()[args.range.fromCell];

        if (!column.editor) {
            return;
        }

        var value = dataView.getItem(args.range.fromRow)[column.field];

        dataView.beginUpdate();

        for (var i = args.range.fromRow + 1; i <= args.range.toRow; i++) {
            dataView.getItem(i)[column.field] = value;
            grid.invalidateRow(i);
        }

        dataView.endUpdate();
        grid.render();
    });

    grid.registerPlugin(overlayPlugin);

    grid.init();
    
    $("#grid").data("gridInstance", grid);


    function filter(item) {
        var columns = grid.getColumns();

        var value = true;

        for (var i = 0; i < columns.length; i++) {
            var col = columns[i];
            var filterValues = col.filterValues;

            if (filterValues && filterValues.length > 0) {
                value = value & _.contains(filterValues, item[col.field]);
            }
        }
        return value;
    }
}

function exportToCsv() {
	var type = decodeURIComponent(urlParam('type'));		
	var name = decodeURIComponent(urlParam('name'));
	var filename = name;
	if(type == 'dataset')
	{
		filename = name + ".csv";
	}
	
	var data = $("#grid").data("gridInstance").getData().getItems();
	var columns = $("#grid").data("gridInstance").getColumns();
	
    var processRow = function (row,columns) {
        var finalVal = '';
        var j = 0;
        for (var i = 0; i < columns.length; i++) 
        {
        	var value = '';
            if(row.hasOwnProperty(columns[i].field)) {
                value = row[columns[i].field];
            }
 
            var innerValue = value === null ? '' : value.toString();
            if (value instanceof Date) {
                innerValue = value.toLocaleString();
            }
            var result = innerValue.replace(/"/g, '""');
            if (result.search(/("|,|\n)/g) >= 0)
                result = '"' + result + '"';
 
    		if (i > 0)
                finalVal += ',';
            finalVal += result;
        }
        return finalVal + '\n';
    };

    var csvFile = '';
    var headerVal = '';
    for (var i = 0; i < columns.length; i++) 
    {
		if (i > 0)
			headerVal += ',';
		headerVal += columns[i].name;
    }
	headerVal += '\n'; 
	csvFile += headerVal;
    
    for (var i = 0; i < data.length; i++) 
    {
    	csvFile += processRow(data[i],columns);
    }

    var blob = new Blob([csvFile], { type: 'text/csv;charset=utf-8;' });
    if (navigator.msSaveBlob) { // IE 10+
        navigator.msSaveBlob(blob, filename);
    } else {
        var link = document.createElement("a");
        if (link.download !== undefined) { // feature detection
            // Browsers that support HTML5 download attribute
            var url = URL.createObjectURL(blob);
            link.setAttribute("href", url);
            link.setAttribute("download", filename);
            link.style.visibility = 'hidden';
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
        }
    }
}

function isEmpty(str) {
    return (!str || 0 === str.length);
}