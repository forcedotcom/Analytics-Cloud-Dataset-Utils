(function ($) {
    $.extend(true, window, {
        "Ext": {
            "Plugins": {
                "HeaderFilter": HeaderFilter
            }
        }
    });

    /*
    Based on SlickGrid Header Menu Plugin (https://github.com/mleibman/SlickGrid/blob/master/plugins/slick.headermenu.js)

    (Can't be used at the same time as the header menu plugin as it implements the dropdown in the same way)
    */

    function HeaderFilter(options) {
        var grid;
        var self = this;
        var handler = new Slick.EventHandler();
        var defaults = {
            buttonImage: "slickgrid/images/down.png",
            filterImage: "slickgrid/images/filter.png",
            sortAscImage: "slickgrid/images/sort-asc.png",
            sortDescImage: "slickgrid/images/sort-desc.png"
        };
        var $menu;

        function init(g) {
            options = $.extend(true, {}, defaults, options);
            grid = g;
            handler.subscribe(grid.onHeaderCellRendered, handleHeaderCellRendered)
                   .subscribe(grid.onBeforeHeaderCellDestroy, handleBeforeHeaderCellDestroy)
                   .subscribe(grid.onClick, handleBodyMouseDown)
                   .subscribe(grid.onColumnsResized, columnsResized);

            grid.setColumns(grid.getColumns());

            $(document.body).bind("mousedown", handleBodyMouseDown);
        }

        function destroy() {
            handler.unsubscribeAll();
            $(document.body).unbind("mousedown", handleBodyMouseDown);
        }

        function handleBodyMouseDown(e) {
            if ($menu && $menu[0] != e.target && !$.contains($menu[0], e.target)) {
                hideMenu();
            }
        }

        function hideMenu() {
            if ($menu) {
                $menu.remove();
                $menu = null;
            }
        }

        function handleHeaderCellRendered(e, args) {
            var column = args.column;

            var $el = $("<div></div>")
                .addClass("slick-header-menubutton")
                .data("column", column);

            if (options.buttonImage) {
                $el.css("background-image", "url(" + options.buttonImage + ")");
            }

            $el.bind("click", showFilter).appendTo(args.node);
        }

        function handleBeforeHeaderCellDestroy(e, args) {
            $(args.node)
                .find(".slick-header-menubutton")
                .remove();
        }

        function addMenuItem(menu, columnDef, title, command, image) {
            var $item = $("<div class='slick-header-menuitem'>")
                         .data("command", command)
                         .data("column", columnDef)
                         .bind("click", handleMenuItemClick)
                         .appendTo(menu);

            var $icon = $("<div class='slick-header-menuicon'>")
                         .appendTo($item);

            if (image) {
                $icon.css("background-image", "url(" + image + ")");
            }

            $("<span class='slick-header-menucontent'>")
             .text(title)
             .appendTo($item);
        }

        function addMenuInput(menu, columnDef) {
            $("<input class='input' placeholder='Search' style='margin-top: 5px; width: 206px'>")
                .data("column", columnDef)
                .bind("keyup", function (e) {
                    var filterVals = getFilterValuesByInput($(this));
                    updateFilterInputs(menu, columnDef, filterVals);
                })
                .appendTo(menu);
        }

        function updateFilterInputs(menu, columnDef, filterItems) {
            var filterOptions = "<label><input type='checkbox' value='-1' />(Select All)</label>";
            columnDef.filterValues = columnDef.filterValues || [];

            // WorkingFilters is a copy of the filters to enable apply/cancel behaviour
            workingFilters = columnDef.filterValues.slice(0);

            for (var i = 0; i < filterItems.length; i++) {
                var filtered = _.contains(workingFilters, filterItems[i]);

                filterOptions += "<label><input type='checkbox' value='" + i + "'"
                + (filtered ? " checked='checked'" : "")
                + "/>" + filterItems[i] + "</label>";
            }
            var $filter = menu.find('.filter');
            $filter.empty().append($(filterOptions));

            $(':checkbox', $filter).bind('click', function () {
                workingFilters = changeWorkingFilter(filterItems, workingFilters, $(this));
            });
        }

        function showFilter(e) {
            var $menuButton = $(this);
            var columnDef = $menuButton.data("column");

            columnDef.filterValues = columnDef.filterValues || [];

            // WorkingFilters is a copy of the filters to enable apply/cancel behaviour
            var workingFilters = columnDef.filterValues.slice(0);

            var filterItems;

            if (workingFilters.length === 0) {
                // Filter based all available values
                filterItems = getFilterValues(grid.getData(), columnDef);
            }
            else {
                // Filter based on current dataView subset
                filterItems = getAllFilterValues(grid.getData().getItems(), columnDef);
            }

            if (!$menu) {
                $menu = $("<div class='slick-header-menu'>").appendTo(document.body);
            }

            $menu.empty();

            addMenuItem($menu, columnDef, 'Sort Ascending', 'sort-asc', options.sortAscImage);
            addMenuItem($menu, columnDef, 'Sort Descending', 'sort-desc', options.sortDescImage);
            addMenuInput($menu, columnDef);

            var filterOptions = "<label><input type='checkbox' value='-1' />(Select All)</label>";

            for (var i = 0; i < filterItems.length; i++) {
                var filtered = _.contains(workingFilters, filterItems[i]);

                filterOptions += "<label><input type='checkbox' value='" + i + "'"
                                 + (filtered ? " checked='checked'" : "")
                                 + "/>" + filterItems[i] + "</label>";
            }

            var $filter = $("<div class='filter'>")
                           .append($(filterOptions))
                           .appendTo($menu);

            $('<button>OK</button>')
                .appendTo($menu)
                .bind('click', function (ev) {
                    columnDef.filterValues = workingFilters.splice(0);
                    setButtonImage($menuButton, columnDef.filterValues.length > 0);
                    handleApply(ev, columnDef);
                });

            $('<button>Clear</button>')
                .appendTo($menu)
                .bind('click', function (ev) {
                    columnDef.filterValues.length = 0;
                    setButtonImage($menuButton, false);
                    handleApply(ev, columnDef);
                });

            $('<button>Cancel</button>')
                .appendTo($menu)
                .bind('click', hideMenu);

            $(':checkbox', $filter).bind('click', function () {
                workingFilters = changeWorkingFilter(filterItems, workingFilters, $(this));
            });

            var offset = $(this).offset();
            var left = offset.left - $menu.width() + $(this).width() - 8;

            var menutop = offset.top + $(this).height();

            if (menutop + offset.top > $(window).height()) {
                menutop -= ($menu.height() + $(this).height() + 8);
            }
            $menu.css("top", menutop)
                 .css("left", (left > 0 ? left : 0));
        }

        function columnsResized() {
            hideMenu();
        }

        function changeWorkingFilter(filterItems, workingFilters, $checkbox) {
            var value = $checkbox.val();
            var $filter = $checkbox.parent().parent();

            if ($checkbox.val() < 0) {
                // Select All
                if ($checkbox.prop('checked')) {
                    $(':checkbox', $filter).prop('checked', true);
                    workingFilters = filterItems.slice(0);
                } else {
                    $(':checkbox', $filter).prop('checked', false);
                    workingFilters.length = 0;
                }
            } else {
                var index = _.indexOf(workingFilters, filterItems[value]);

                if ($checkbox.prop('checked') && index < 0) {
                    workingFilters.push(filterItems[value]);
                }
                else {
                    if (index > -1) {
                        workingFilters.splice(index, 1);
                    }
                }
            }

            return workingFilters;
        }

        function setButtonImage($el, filtered) {
            var image = "url(" + (filtered ? options.filterImage : options.buttonImage) + ")";

            $el.css("background-image", image);
        }

        function handleApply(e, columnDef) {
            hideMenu();

            self.onFilterApplied.notify({ "grid": grid, "column": columnDef }, e, self);

            e.preventDefault();
            e.stopPropagation();
        }

        function getFilterValues(dataView, column) {
            var seen = [];
            for (var i = 0; i < dataView.getLength() ; i++) {
                var value = dataView.getItem(i)[column.field];

                if (!_.contains(seen, value)) {
                    seen.push(value);
                }
            }

            return _.sortBy(seen, function (v) { return v; });
        }

        function getFilterValuesByInput($input) {
            var column = $input.data("column"),
                filter = $input.val(),
                dataView = grid.getData(),
                seen = [];

            for (var i = 0; i < dataView.getLength() ; i++) {
                var value = dataView.getItem(i)[column.field];

                if (filter.length > 0) {
                    var mVal = !value ? '' : value;
                    var lowercaseFilter = filter.toString().toLowerCase();
                    var lowercaseVal = mVal.toString().toLowerCase();
                    if (!_.contains(seen, value) && lowercaseVal.indexOf(lowercaseFilter) > -1) {
                        seen.push(value);
                    }
                }
                else {
                    if (!_.contains(seen, value)) {
                        seen.push(value);
                    }
                }
            }

            return _.sortBy(seen, function (v) { return v; });
        }

        function getAllFilterValues(data, column) {
            var seen = [];
            for (var i = 0; i < data.length; i++) {
                var value = data[i][column.field];

                if (!_.contains(seen, value)) {
                    seen.push(value);
                }
            }

            return _.sortBy(seen, function (v) { return v; });
        }

        function handleMenuItemClick(e) {
            var command = $(this).data("command");
            var columnDef = $(this).data("column");

            hideMenu();

            self.onCommand.notify({
                "grid": grid,
                "column": columnDef,
                "command": command
            }, e, self);

            e.preventDefault();
            e.stopPropagation();
        }

        $.extend(this, {
            "init": init,
            "destroy": destroy,
            "onFilterApplied": new Slick.Event(),
            "onCommand": new Slick.Event()
        });
    }
})(jQuery);