$(document).ready(function() {        
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

	preview(type,name);	
});


$(document).ajaxSend(function(event, request, settings) {
		  $('#loading-indicator').show();
		});

$(document).ajaxComplete(function(event, request, settings) {
		  $('#loading-indicator').hide();
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

function preview(type,name)
{
	full_url = "/preview?type="+type+"&name="+name;
	$.getJSON(full_url, {}, function(data) {
				var columns = data.columns;
				var pdata = data.data
				buildGrid(columns,pdata);
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
	              	$("#title2").append('').html("<h5 style='text-align:center'><i style='color:#FF0000'>"+err.statusMessage+"</i></h5>");
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


function isEmpty(str) {
    return (!str || 0 === str.length);
}