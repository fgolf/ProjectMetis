var json_file = "web_summary.json";
var alldata = {};
var detailsVisible = false;

google.charts.load('current', {'packages':['corechart']});
google.charts.setOnLoadCallback(function() {console.log("google loaded!");});

function getEscapedTaskID(dsname, tag) {
    return dsname.replace(/\//g,"_")+"_"+tag.replace(/\ /g,"_").replace(/\./g,"p");
}

Array.prototype.sum2 = function (prop, prop2) {
    // So you can do array.sum(keyname)
    var total = 0;
    for ( var i = 0, _len = this.length; i < _len; i++ ) {
        total += this[i][prop][prop2];
    }
    return total;
}

function getProgress(general) {
    var type = general["type"];
    var stat = general["status"];
    var done = general["njobs_done"];
    var tot = general["njobs_total"];

    var pct = 0;
    if (tot > 0) pct = 100.0*done/tot;
    return {
        pct: pct,
        done: done,
        total: tot,
        };
}

function syntaxHighlight(json) {
    // stolen from http://stackoverflow.com/questions/4810841/how-can-i-pretty-print-json-using-javascript
    json = json.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    return json.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, function (match) {
        var cls = 'number';
        if (/^"/.test(match)) {
            if (/:$/.test(match)) {
                cls = 'key';
            } else {
                cls = 'string';
            }
        } else if (/true|false/.test(match)) {
            cls = 'boolean';
        } else if (/null/.test(match)) {
            cls = 'null';
        }
        return '<span class="has-dark ' + cls + '">' + match + '</span>';
    });
}

function loadJSON() {
    $.getJSON(json_file, function(data) { 
        if(!("tasks" in alldata) || (data["tasks"].length != alldata["tasks"].length)) {
            console.log(data);
            setUpDOM(data);
        }
        fillDOM(data); 
    });
}

function setUpDOM(data) {
    var container = $("#tasks-container");
    container.empty(); // clear the section

    for(var i = 0; i < data["tasks"].length; i++) {
        var sample = data["tasks"][i];
        var general = data["tasks"][i]["general"];
        var id = getEscapedTaskID(general["dataset"],general["tag"]);
        // console.log(id);

        var sample_toshow = $.extend(true, {}, sample);
        delete sample_toshow["history"];
        var jsStr = syntaxHighlight(JSON.stringify(sample_toshow, undefined, 4));

        // turn dataset into a DIS link
        var link =  "http://uaf-2.t2.ucsd.edu/~fgolf/dis/?type=basic&short=true&query="+general["dataset"];
        var link_handler =  "http://uaf-2.t2.ucsd.edu:8887/dis/serve?type=basic&short=true&query="+general["dataset"];
        jsStr = jsStr.replace("\"dataset\":", 
            ` <a href="${link}" style="text-decoration: underline" title="<iframe src='${link_handler}' style='background-color: #fff; width:650px;'></iframe>" data-html="true" data-toggle="tooltip">dataset</a>: `
        );
        var typenotask = general["type"].replace("Task","");

        var typebadge = "badge-primary";
        if (Math.floor(getProgress(general).pct) < 100) {
            typebadge = "badge-danger";
        }
        var content = `
            <div id="${id}" data-type="${typenotask}" data-tag="${general['tag']}" class="task">
                <div class="row task-text-row">
                    <a href="#" data-which="type" class="badge task-badge has-dark ${typebadge}">${typenotask}</a>
                    <a href="#" data-which="tag" class="badge task-badge badge-secondary">${general["tag"]}</a>
                    <div class="progress has-dark">
                        <div class="progress-bar" role="progressbar" aria-valuenow="0" aria-valuemin="0" aria-valuemax="100" style="width: 0%;">
                        </div>
                        <span class="progress-type"></span>
                        <span title="" data-toggle="tooltip" class="progress-completed">0%</span>
                    </div>
                    <a href="#!" class="dataset-label badge badge-light has-dark">${general["dataset"]}</a>
                </div>
                <div class="row details has-dark" style="display:none;">${jsStr}</div>
            </div>
            `;
        // console.log(content);
        container.append(content);

    }


}

function fillDOM(data, theme=0) {
    alldata = data;
    // console.log("here: "+alldata);

    var date = new Date(data["last_updated"]*1000); // ms to s
    var hours = date.getHours();
    var minutes = "0" + date.getMinutes();
    var seconds = "0" + date.getSeconds();
    var formattedTime = hours + ':' + minutes.substr(-2) + ':' + seconds.substr(-2);
    $("#last_updated").text("Last updated at " + date.toLocaleTimeString() + " on " + date.toLocaleDateString());

    for(var i = 0; i < data["tasks"].length; i++) {

        var sample = data["tasks"][i];
        var bad = data["tasks"][i]["bad"] || {};
        var general = data["tasks"][i]["general"];
        var id = getEscapedTaskID(general["dataset"],general["tag"]);

        var progress = getProgress(general);
        var pct = Math.floor(progress.pct);
        var pdiv = $("#"+id+" > .task-text-row > .progress")
        var pbar = pdiv.find(".progress-bar");
        var pleft = pdiv.find(".progress-type");
        var pright = pdiv.find(".progress-completed");
        var h = Math.round(pct*1.35,2);
        var s = Math.round(75-0.01*pct*10 + 15.0*Math.max(1.0-(pct-33)*(pct-33)/4500.0, 0.0),0);
        var v = Math.round(58-0.01*pct*14,0);
        var color = `hsl(${h},${s}%,${v}%)`;

        $("#"+id).attr("data-pct",progress.pct);
        $("#"+id).attr("data-tag",general["tag"]);

        if (general["open_dataset"]) {
            color = "#ffaa3b";
        }
        // console.log(color);

        if (theme == 1) {
            // BLUE
            if (pct > 6.0/7*100) color = "#039BE5";
            else if (pct > 5.0/7*100) color = "#03A9F4";
            else if (pct > 4.0/7*100) color = "#29B6F6";
            else if (pct > 3.0/7*100) color = "#4FC3F7";
            else if (pct > 2.0/7*100) color = "#81D4FA";
            else if (pct > 1.0/7*100) color = "#B3E5FC";
            else if (pct > 0.0/7*100) color = "#E1F5FE";
        }

        if (theme == 2) {
            // ORANGE
            if      (pct > 9.0/10*100) color = "#FF6F00";
            else if (pct > 8.0/10*100) color = "#FF8F00";
            else if (pct > 7.0/10*100) color = "#FFA000";
            else if (pct > 6.0/10*100) color = "#FFB300";
            else if (pct > 5.0/10*100) color = "#FFC107";
            else if (pct > 4.0/10*100) color = "#FFCA28";
            else if (pct > 3.0/10*100) color = "#FFD54F";
            else if (pct > 2.0/10*100) color = "#FFE082";
            else if (pct > 1.0/10*100) color = "#FFECB3";
            else if (pct > 0.0/10*100) color = "#FFF8E1";
        }

        // console.log(color);

        pbar.attr("aria-valuenow",pct);
        if (general["open_dataset"]) {
            pbar.addClass("progress-bar-striped");
            // pbar.addClass("progress-bar-animated");
        }
        // https://stackoverflow.com/questions/7069167/css-transition-not-firing
        pbar.css('width');
        pbar.css({
            "width":pct+"%",
            "background-color":color,
        });
        if ("event_rate" in general && general["event_rate"] > 0) {
            if (general["event_rate"] > 200) {
                pleft.html("<small>"+Math.round(general["nevents_done"]/1.0e5)/10 + "M @ " + Math.round(general["event_rate"]/10,2)/100+" kHz"+"</small>");
            } else {
                // pleft.text(general["event_rate"]+" Hz"); 
                pleft.html("<small>"+Math.round(general["nevents_done"]/1.0e5)/10 + "M @ " + general["event_rate"]+" Hz"+"</small>"); 
            }
        }
        // pleft.css({
        //     "color": "#fff",
        // });
        // pright.css({
        //     "color": "#ffff",
        // });
        if (general["open_dataset"]) {
            pright.html("<small>open</small> "+pct+"%");
        } else {
            // pright.text(pct+"%");
            pright.text(pct+"%");
        }
        pright.attr({"title":progress.done + "/" + progress.total});

    }

    showAllTasks();
    updateSummary(data);
    afterFillDOM();

}

function afterFillDOM() {

    // enable tooltips
    $('[data-toggle="tooltip"]').tooltip();

    // clicking on the dataset name toggles the corresponding details panel
    $('.dataset-label').unbind().click(function() {
        $(this).parent().parent().find(".details").slideToggle(100);
    });

    // clicking on a task badge (right now, either the task type or task tag)
    // will show only tasks with the same type or tag. click again to revert.
    $(".task-badge").unbind().click(function() {
        var which = $(this).data("which");
        var val = $(this).text().replace(/\./g,"\\.");
        $(`.task[data-${which}!=${val}]`).toggle();
        var nvis = $.unique($(".task-badge:visible").parent()).length;
        var ntasks = $(".task").length;
        if (nvis != ntasks) {
            $("#nav-taskbadgefilter").text(`${ntasks-nvis} tasks hidden`);
            $("#nav-taskbadgefilter").show();
        } else {
            $("#nav-taskbadgefilter").hide();
        }
        $("#nav-summary-ntasks").text(` (${nvis})`);
    });

}

function showAllTasks() {
    $(".task").show();
    $("#nav-taskbadgefilter").hide();
    var nvis = $.unique($(".task-badge:visible").parent()).length;
    $("#nav-summary-ntasks").text(` (${nvis})`);
}

function updateSummary(data) {
    var nevents_total = 0;
    var nevents_done = 0;
    var njobs_done = 0;
    var njobs_total = 0;
    for(var i = 0; i < data["tasks"].length; i++) {
        var sample = data["tasks"][i];
        var general = sample["general"];
        var id = getEscapedTaskID(general["dataset"], general["tag"]);

        if ($(`#${id}`).is(":hidden")) continue;
        nevents_total += general["nevents_total"];
        nevents_done += general["nevents_done"];
        njobs_total += general["njobs_total"];
        njobs_done += general["njobs_done"];

    }

    // var nevents_total = data["tasks"].sum2("general","nevents_total");
    // var nevents_done = data["tasks"].sum2("general","nevents_done");
    // var njobs_done = data["tasks"].sum2("general","njobs_done");
    // var njobs_total = data["tasks"].sum2("general","njobs_total");

    var pct_events = Math.round(100.0*100.0*nevents_done/nevents_total)/100;
    var pct_jobs = Math.round(100.0*100.0*njobs_done/njobs_total)/100;

    // var buff = `<table>
    var buff = `<table class="table table-sm" style="width: 30%; font-size: 75%;">
                    <tbody>
                <tr><th align='left' style="padding-top:0px; padding-bottom:0px;">Nevents (total)  </th> <td align='right' style="padding-top:0px;padding-bottom:0px;">${nevents_total.toLocaleString()}             </td></tr>
                <tr><th align='left' style="padding-top:0px; padding-bottom:0px;">Nevents (done)   </th> <td align='right' style="padding-top:0px;padding-bottom:0px;">${nevents_done.toLocaleString()}              </td></tr>
                <tr><th align='left' style="padding-top:0px; padding-bottom:0px;">Nevents (missing)</th> <td align='right' style="padding-top:0px;padding-bottom:0px;">${(nevents_total-nevents_done).toLocaleString()}</td></tr>
                <tr><th align='left' style="padding-top:0px; padding-bottom:0px;">Njobs (total)    </th> <td align='right' style="padding-top:0px;padding-bottom:0px;">${njobs_total.toLocaleString()}               </td></tr>
                <tr><th align='left' style="padding-top:0px; padding-bottom:0px;">Njobs (done)     </th> <td align='right' style="padding-top:0px;padding-bottom:0px;">${njobs_done.toLocaleString()}                </td></tr>
                <tr><th align='left' style="padding-top:0px; padding-bottom:0px;">Njobs (running)  </th> <td align='right' style="padding-top:0px;padding-bottom:0px;">${(njobs_total-njobs_done).toLocaleString()}    </td></tr>
                <tr><th align='left' style="padding-top:0px; padding-bottom:0px;">Event completion </th> <td align='right' style="padding-top:0px;padding-bottom:0px;">${pct_events}%               </td></tr>
                <tr><th align='left' style="padding-top:0px; padding-bottom:0px;">Job completion   </th> <td align='right' style="padding-top:0px;padding-bottom:0px;">${pct_jobs}%                 </td></tr>
        </tbody>
    </table>`;
    $("#summary").html(buff);
    document.title = `[${Math.round(pct_jobs)}] Metis Dashboard`;
}

function doHistory() {
    var tot_history = {};
    console.log(alldata);
    for(var i = 0; i < alldata["tasks"].length; i++) {
        var history = alldata["tasks"][i]["history"] || {};
        // Don't consider things in the chart if they are hidden
        var general = alldata["tasks"][i]["general"];
        var id = getEscapedTaskID(general["dataset"],general["tag"]);
        if ($('#'+id).is(":hidden")) {
            continue;
        }
        if (!("timestamps" in history)) continue;
        for(var j = 0; j < history["timestamps"].length; j++) {
            var ts = history["timestamps"][j];
            for (k in history) {
                if (k == "timestamps") continue;
                var val = history[k][j];
                if (!(ts in tot_history)) tot_history[ts] = {};
                if (!(k in tot_history[ts])) tot_history[ts][k] = 0;
                tot_history[ts][k] += val;
            }
        }
    }
    console.log(tot_history);
    drawChart(tot_history);
}

function drawChart(history) {

    var data_table = [ [
        'timestamp',
        'jobs completed ',
        'jobs left',
        'jobs total',
        // 'events completed ',
        // 'events total',
    ] ];
        
    // for (var itd = 0; itd < history["time_stats"].length; itd++) {
    for (ts in history) {
        var td = history[ts];

        data_table.push( [
            new Date(ts*1000), // to ms
            td["njobs_done"] ,
            td["njobs_total"]-td["njobs_done"],
            td["njobs_total"],
            // td["nevents_done"] ,
            // td["nevents_total"],
        ] );
    }

    console.log(data_table);
    var data = google.visualization.arrayToDataTable(data_table);
    var options_stacked = {
        height: 450,
        width: 850,
        legend: {position: 'right'},
        // vAxis: {minValue: 0, title: "jobs"},
        vAxis: {title: "jobs"},
        hAxis: {slantedText:true, gridlines:{count:-1}},
        // vAxis: {logScale: true},
    };
    var chart = new google.visualization.AreaChart(document.getElementById('chart'));
    // The select handler. Call the chart's getSelection() method
    function selectHandler() {
        var selectedItem = chart.getSelection()[0];
        if (selectedItem == null) {
            console.log("you probably clicked on the legend, so making y-axis logscale");
        }
        console.log(selectedItem);
        options_stacked.vAxis.logScale ^= true;
        chart.draw(data, options_stacked);
    }
    // Listen for the 'select' event, and call my function selectHandler() when
    // the user selects something on the chart.
    google.visualization.events.addListener(chart, 'select', selectHandler);
    // console.log(chart);
    chart.draw(data, options_stacked);
}


function toggleSummary() {
    if ($('#summary').is(":hidden")) {
        updateSummary(alldata);
    }
    $("#summary").slideToggle(150);
    $("#nav-summary").toggleClass("active");
}

function toggleChart() {
    if ($('#chart').is(":hidden")) {
        doHistory(alldata);
    }
    $("#nav-chart").toggleClass("active");
    $("#chart").slideToggle(150);
}

function toggleExpand() {
    // do it this way because one guy may be reversed
    if(detailsVisible) {
        $("#nav-expand").text("Expand");
        $("#nav-expand").removeClass("active");
    } else {
        $("#nav-expand").text("Collapse");
        $("#nav-expand").addClass("active");
    }
    $(".details").toggle();
    detailsVisible = !detailsVisible;
}

function sortSamples(which) {
    var divs = $(".task");
    if (which == "az") {
        var ordered = divs.sort(function (a, b) {
            return $(a).attr('id') < $(b).attr('id') ? -1 : 1;
        });
        $("#tasks-container").html(ordered);
    } else if (which == "pct") {
        var ordered = divs.sort(function (a, b) {
            return $(a).data('pct') < $(b).data('pct') ? -1 : 1;
        });
        $("#tasks-container").html(ordered);
    } else if (which == "tag") {
        var ordered = divs.sort(function (a, b) {
            return $(a).data('tag') < $(b).data('tag') ? -1 : 1;
        });
        $("#tasks-container").html(ordered);
    } else if (which == "rev") {
        $("#tasks-container").html(divs.get().reverse());
    } else if (which == "era") {
        var ordered = divs.sort(function (a, b) {
            return $(a).attr('id').split("_Run")[1] < $(b).attr('id').split("_Run")[1] ? -1 : 1;
        });
        $("#tasks-container").html(ordered);
    }
    afterFillDOM();
}

function toggleSort(which) {
    sortSamples(which);
}

function toggleAdmin() {
    $("#nav-admin").toggleClass("active");
    $(".progress-type").text("🛑✉️✂️");
}

function toggleDarkMode() {
    $(".has-dark").toggleClass("dark");
    $(".has-dark.badge").toggleClass("badge-light").toggleClass("badge-dark")
    console.log($(".has-dark > .badge-light"));
    $("#nav-dark").toggleClass("active");
}


$(function() {
    loadJSON();
});
