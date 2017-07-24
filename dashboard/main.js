var alldata = {};
var jsonFile = "web_summary.json";
var baseDir = '/home/users/namin/2017/fourtop/babymaking/batch/NtupleTools/AutoTwopler/';
var refreshSecs = 10*60;
var detailsVisible = false;
var sortedAZ = false;
var duckMode = false;
var adminMode = false;

// google.charts.load('current', {'packages':['corechart']});
// google.charts.setOnLoadCallback(function() {console.log("google loaded!");});
// google.charts.setOnLoadCallback(drawChart);

// google.charts.setOnLoadCallback(function() {
//     $(function() { 
//         console.log("loading chart!");
//         drawChart(); 
//     });
// });

// So you can do array.sum(keyname)
Array.prototype.sum = function (prop) {
    var total = 0;
    for ( var i = 0, _len = this.length; i < _len; i++ ) {
        total += this[i][prop]
    }
    return total;
}
// So you can do array.sum(keyname)
Array.prototype.sum2 = function (prop, prop2) {
    var total = 0;
    for ( var i = 0, _len = this.length; i < _len; i++ ) {
        total += this[i][prop][prop2];
    }
    return total;
}
// console.log("Hello, {0}!".format("World"))
String.prototype.format = function() {
  a = this;
  for (k in arguments) {
    a = a.replace("{" + k + "}", arguments[k])
  }
  return a
}

$(function() {

    // https://stackoverflow.com/questions/2419749/get-selected-elements-outer-html
    // when selecting a div.html() with jquery, you lose the text for the div itself
    // (you only get what's inside a div, so we need outerHTML to get everything)
    (function($) {
        $.fn.outerHTML = function() {
            return $(this).clone().wrap('<div></div>').parent().html();
        };
    })(jQuery);

    loadJSON();
    setInterval(loadJSON, refreshSecs*1000);

    handleDuckMode();
    handleOtherTwiki();
    handleSubmitButton();
    handleAdminMode();

});

$.ajaxSetup({
   type: 'POST',
   timeout: 15000,
});

function handleSubmitButton() {
    $('.submitButton').click(function (e) {
        if (e.target) {
            if(e.target.value == "fetch" || e.target.value == "update") {
                doTwiki(e.target.value);
            } else if(e.target.value == "addinstructions") {
                // console.log("adding to instructions");
                addInstructions(e.target.value);
            }
        }
    });
}

function handleOtherTwiki() {
    $( "#selectPage" ).change(function() {
        if($(this).find(":selected").text()=="Other") {
            $("#otherPage").show();
        } else {
            $("#otherPage").hide();
        }
    });
}

function handleAdminMode() {
    $( "#firstTitle" ).click(function() { 
        $( "#admin" ).fadeToggle(150); 
        if(adminMode) {
            adminMode = false;
            fillDOM(alldata);
        } else {
            adminMode = true;
            fillDOM(alldata);
        }
    });
}

function handleDuckMode() {
    $( ".mainlogo" ).on('contextmenu dblclick',function() { 
        if(duckMode) {
            duckMode = false;
            $(".mainlogo").attr('src', 'images/workflowtransparent.png');
            $("#container").css("background", "");
            $("#firstTitle").text("Project");
            $(".duckAudio").trigger('pause');
        } else {
            duckMode = true;
            $(".mainlogo").attr('src', 'images/ducklogo.png');
            $("#container").css("background", "url(images/ducklogo.png");
            $("#firstTitle").text("duck");
            $(".duckAudio").prop("currentTime",0);
            $(".duckAudio").trigger('play');
        }
        fillDOM(alldata);
    });
}


function loadJSON() {
    // $.getJSON("http://uaf-6.t2.ucsd.edu/~namin/dump/test.json", function(data) { parseJson(data); });
    // WOW CAN PUT EXTERNAL URLS HERE MAN!
    $.getJSON(jsonFile, function(data) { 
        if(!("tasks" in alldata) || (data["tasks"].length != alldata["tasks"].length)) {
            setUpDOM(data);
        }
        fillDOM(data); 
        afterFillDOM();
    });
}

function afterFillDOM() {
    // console.log($('.forqtooltip_dis'));

    // $('.fortooltip').each(function() {
    //     $(this).tooltip({ 
    //         track: true 
    //     });
    // });

    $('.forqtooltip_dis').each(function() {
        $(this).qtip({

            content: {
                text: function(event, api) {
                    var context = api.elements.target.context;
                    var hostname = context.hostname;
                    var pathname = context.pathname;
                    var search = context.search;
                    var protocol = context.protocol;
                    var url = protocol+"//"+hostname+pathname+"handler.py"+search;
                    // var url = "http://uaf-8.t2.ucsd.edu/~namin/makers/disMaker/handler.py?type=basic&short=short&query=/JetHT/Run2017C-PromptReco-v1/MINIAOD";
                    $.ajax({
                        // url: api.elements.target.attr('href') // Use href attribute as URL
                        url: url
                    })
                    .then(function(content) {
                        // Set the tooltip content upon successful retrieval
                        var todisplay = content["response"]["payload"];
                        todisplay = syntaxHighlight(JSON.stringify(todisplay, undefined, 4));
                        console.log(todisplay);
                        api.set('content.text', "<pre>"+todisplay+"</pre>");
                    }, function(xhr, status, error) {
                        // Upon failure... set the tooltip content to error
                        api.set('content.text', status + ': ' + error);
                    });
        
                    return 'Loading...'; // Set some initial text
                }
            },

            position: {
                target: 'mouse', // Track the mouse as the positioning target
                adjust: { x: 5, y: 5 } // Offset it slightly from under the mouse
            },
        });
    });

    $('.forqtooltip').each(function() {
        $(this).qtip({

            content: {
                text: $(this).attr("title")
            },
            position: {
                target: 'mouse', // Track the mouse as the positioning target
                adjust: { x: 5, y: 5 } // Offset it slightly from under the mouse
            },
        });
    });

}

function doTwiki(type) {
    $("#twikiTextarea").val("Loading...");
    $("#message").html("");

    var formObj = {};
    formObj["action"] = type;
    if(type == "update") {
        var donesamples = [];
        for(var i = 0; i < alldata["tasks"].length; i++) {
            if(alldata["tasks"][i]["type"] == "BABY") continue;
            
            donesamples.push( alldata["tasks"][i] );
        }
        console.log(donesamples);
        formObj["tasks"] = JSON.stringify(donesamples);
    }
    var inputs = $("#fetchTwikiForm").serializeArray();
    $.each(inputs, function (i, input) {
        formObj[input.name] = input.value;
    });
    console.log(formObj);
    $.ajax({
            url: "./handler.py",
            type: "POST",
            data: formObj,
            success: function(data) {
                    console.log(data);
                    $("#twikiTextarea").val(data);
                },
            error: function(data) {
                    $("#message").html("<span style='color:red'>Error:</span> "+data["responseText"]);
                    console.log(data);
                },
       });
}

function displayMessage(html) {
    $("#message").stop().fadeIn(0).html(html).delay(5000).fadeOut(2000);
}

function addInstructions(type) {
    var formObj = {};
    formObj["action"] = type;
    formObj["data"] = $("#twikiTextarea").val();
    formObj["basedir"] = baseDir;
    console.log(formObj);
    $.ajax({
            url: "./handler.py",
            type: "POST",
            data: formObj,
            success: function(data) {
                    displayMessage("<span style='color:green'>"+data+"</span>")
                    console.log(data);
                },
            error: function(data) {
                    displayMessage("<span style='color:red'>Error:</span> "+data["responseText"])
                    console.log(data);
                },
       });
}


function getProgress(general) {
    var type = general["type"];
    var stat = general["status"];
    var done = general["njobs_done"];
    var tot = general["njobs_total"];

    var pct = 100.0*done/tot;
    return {
        pct: pct,
        done: done,
        total: tot,
        };

    // if (type == "CMS3") {
    //     if (stat == "new") return 0.0;
    //     else if (stat == "crab") {
    //         if("breakdown" in sample["crab"]) {
    //             done = sample["crab"]["breakdown"]["finished"];
    //             tot = sample["crab"]["njobs"];
    //             if(tot < 1) tot = 1;
    //         }
    //         return 0.0 + 65.0*(done/tot);
    //     } else if (stat == "postprocessing") {
    //         if("postprocessing" in sample) {
    //             done = sample["postprocessing"]["done"];
    //             tot = sample["postprocessing"]["total"];
    //         }
    //         return 68.0 + 30.0*(done/tot);
    //     } else if (stat == "done") return 100;
    //     else return -1.0;
    // } else if(type == "BABY") {
    //     done = sample["baby"]["sweepRooted"];
    //     tot = sample["baby"]["total"];
    //     var babypct = 99.0*done/tot;
    //     if (stat == "done") babypct = 100.0;
    //     return babypct;
    // }

}

function doSendAction(type, dsescaped) {
    var dataset = dsescaped.replace(/\_/g,"/");
    console.log("action,dataset: " + type + " " + dataset);

    if (!confirm('Are you sure you want to do the action: ' + type)) return;

    var obj = {};
    obj["action"] = "action";
    obj["action_type"] = type;
    obj["dataset"] = dataset;
    obj["basedir"] = baseDir;
    console.log(obj);
    $.ajax({
            url: "./handler.py",
            type: "POST",
            data: obj,
            success: function(data) {
                    displayMessage("<span style='color:green'>"+data+"</span>")
                    console.log(data);
                },
            error: function(data) {
                    displayMessage("<span style='color:red'>Error:</span> "+data["responseText"])
                    console.log(data);
                },
       });
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
        return '<span class="' + cls + '">' + match + '</span>';
    });
}

function setUpDOM(data) {
    var container = $("#section_1");
    container.empty(); // clear the section


    for(var i = 0; i < data["tasks"].length; i++) {
        var general = data["tasks"][i]["general"];
        var toappend = "";
        var id = general["dataset"].replace(/\//g,"_");
        // toappend += "<br>\n";
        toappend += "<div id='"+id+"' class='sample'>\n";
        toappend += "<a href='#/' class='thick' onClick=\"$('#details_"+id+"').slideToggle(100)\">";
        if(general["type"] == "BABY") {
            toappend += "<span style='color: purple'>[&#128700; "+general["baby"]["analysis"]+" "+general["baby"]["baby_tag"]+"]</span> ";
        } else if (general["type"] == "CMS4") {
            toappend += "<span style='color: purple'>[CMS4]</span> ";
        }
        toappend += general["dataset"]+"</a>";
        toappend += "<div class='pbar' id='pbar_"+id+"'>";
        toappend +=      "<span id='pbartextleft_"+id+"' class='pbartextleft'></span>";
        toappend +=      "<span id='pbartextright_"+id+"' class='pbartextright'></span>";
        toappend += "</div>\n";
        toappend += "<div id='details_"+id+"' style='display:none;' class='details'></div>\n";
        toappend += "</div>\n";

        container.append(toappend);

        $( "#pbar_"+id ).progressbar({max: 100});
        $("#pbar_"+id).progressbar("option","value",0);
    }
}

function fillDOM(data) {
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
        var id = general["dataset"].replace(/\//g,"_");

        var progress = getProgress(general);
        var pct = Math.round(progress.pct);
        var color = 'hsl(' + pct*0.8 + ', 70%, 50%)';
        if(duckMode) {
            color = 'hsl(' + (pct*0.8+50) + ', 70%, 50%)';
        }
        if(pct == 100) {
            // different color if completely done
            color = 'hsl(' + pct*1.2 + ', 70%, 50%)';
        }


        var towrite = general["status"] + " <span class='forqtooltip' title='"+progress.done+"/"+progress.total+"'>[" + pct + "%]</span>";
        // if it's an open dataset, use a universal color and modify text
        if (general["open_dataset"]) {
            towrite = "open, "+towrite;
            color = "#ffaa3b";
        }
        $("#pbar_"+id).progressbar("value", pct);
        $("#pbar_"+id).find(".ui-progressbar-value").css({"background": color});
        $("#pbartextright_"+id).html(towrite);
        $("#pbartextleft_"+id).html(""); 

        if(adminMode) {
            var buff = "";
            buff += "<a href='#/' onClick='doSendAction(\"kill\",\""+id+"\")' title='kill job (not enabled)'> &#9762; </a>  ";
            if(general["type"] == "CMS3") {
                buff += "<a href='#/' onClick='doSendAction(\"skip_tail\",\""+id+"\")' title='skip tail CRAB jobs'> &#9986; </a> ";
                buff += "<a href='#/' onClick='doSendAction(\"repostprocess\",\""+id+"\")' title='re-postprocess'> &#128296; </a> ";
            } else {
                buff += "<a href='#/' onClick='doSendAction(\"baby_skip_tail\",\""+id+"\")' title='skip rest of baby jobs'> &#9986; </a> ";
                buff += "<a href='#/' onClick='doSendAction(\"baby_remerge\",\""+id+"\")' title='remerge'> &#128290; </a> ";
            }
            buff += "<a href='#/' onClick='doSendAction(\"email_done\",\""+id+"\")' title='send email when done'> &#9993; </a> ";

            $("#pbartextleft_"+id).html(buff);
        }

        var beforedetails = "";
        var afterdetails = "";

        var show_plots = true;
        if (show_plots && ("plots" in bad) && (bad["plots"].length > 0)) {
            for (var iplot = 0; iplot < bad["plots"].length; iplot++) {
                var plot = bad["plots"][iplot];
                if (iplot > 0 && iplot%3==0) beforedetails += "<br>";
                beforedetails += "<img src='"+plot+"' width='25%'/>";
            }
            beforedetails += "\n";
        }

        var jsStr = syntaxHighlight(JSON.stringify(sample, undefined, 4));

        // turn dataset into a DIS link
        // var link = "http://uaf-7.t2.ucsd.edu/~namin/makers/disMaker/?type=basic&short=short&query="+general["dataset"];
        var link = "http://uaf-7.t2.ucsd.edu/~namin/makers/disMaker/?type=basic&short=short&query="+general["dataset"];
        jsStr = jsStr.replace("\"dataset\":", " <a href='"+link+"' class='forqtooltip_dis' style='text-decoration: underline'>dataset</a>: ");
        
        $("#details_"+id).html(beforedetails+"<pre>" + jsStr + "</pre>"+afterdetails);

    }


    updateSummary(data);
    // drawChart();
    //




}

function sortSamples(alphabetical=false) {

    // console.log(alldata);
    alldata["tasks"].sort(function (a,b) {
        var cmp = 0;
        var ageneral = a["general"]; 
        var bgeneral = b["general"]; 
        if (alphabetical) {
            cmp = ageneral["dataset"] > bgeneral["dataset"];
        } else {
            // console.log(ageneral);
            var aprogress = getProgress(ageneral).pct;
            var bprogress = getProgress(bgeneral).pct;
            cmp = aprogress > bprogress;
        }
        if (cmp) return 1;
        else return -1;
    });
    setUpDOM(alldata);
    fillDOM(alldata);

    // var divs = $(".sample");
    // divs.sort(function(a, b){
    //     var comp = 0;
    //     if (alphabetical) comp = $(a).attr("id")>$(b).attr("id");
    //     else {
    //         // XXX NOTE
    //         // after we sort once, we can't do it again
    //         // because when we do $section1.html(blah), we kill all the 
    //         // progress bar objects, so ("value") doesn't work anymore
    //         // console.log($(a).find("div[id^='pbar']"));
    //         // console.log($(b).find("div[id^='pbar']"));
    //         // console.log($(a).find("div[id^='pbar']").progressbar("value"));
    //         comp = $(a).find("div[id^='pbar']").progressbar("value") > $(b).find("div[id^='pbar']").progressbar("value");
    //     }
    //     if (comp) {
    //         return 1;
    //     } else {
    //         return -1;
    //     }
    // });
    // var buff = "";
    // for (var i = 0; i < divs.length; i++) {
    //     buff += divs.eq(i).outerHTML();
    // }
    // $("#section_1").html(buff);

}

function toggleSort() {
    if(sortedAZ) {
        $("#toggle_sort").text("sort %");
    } else {
        $("#toggle_sort").text("sort a-z")
    }
    sortedAZ = !sortedAZ;
    console.log(sortedAZ);
    sortSamples(!sortedAZ);
}

function updateSummary(data) {
    // var total_dict = {};
    for(var i = 0; i < data["tasks"].length; i++) {
        var sample = data["tasks"][i];
        var bad = data["tasks"][i]["bad"] || {};
        var general = data["tasks"][i]["general"];
        // console.log(bad);
        // total_dict += general;
    }
    // console.log(general);

    var nevents_total = data["tasks"].sum2("general","nevents_total");
    var nevents_done = data["tasks"].sum2("general","nevents_done");
    var njobs_done = data["tasks"].sum2("general","njobs_done");
    var njobs_total = data["tasks"].sum2("general","njobs_total");

    var buff = "";
    buff += "<table>";
    buff += "<tr><th align='left'>Nevents (total)</th> <td align='right'>{0}</td></tr>".format(nevents_total);
    buff += "<tr><th align='left'>Nevents (done)</th> <td align='right'>{0}</td></tr>".format(nevents_done);
    buff += "<tr><th align='left'>Nevents (missing)</th> <td align='right'>{0}</td></tr>".format(nevents_total-nevents_done);
    buff += "<tr><th></th><td></td></tr>";
    buff += "<tr><th align='left'>Njobs (total)</th> <td align='right'>{0}</td></tr>".format(njobs_total);
    buff += "<tr><th align='left'>Njobs (done)</th> <td align='right'>{0}</td></tr>".format(njobs_done);
    buff += "<tr><th align='left'>Njobs (running)</th> <td align='right'>{0}</td></tr>".format(njobs_total-njobs_done);
    buff += "<tr><th></th><td></td></tr>";
    buff += "<tr><th align='left'>Event completion</th> <td align='right'>{0}%</td></tr>".format(Math.round(100.0*100.0*nevents_done/nevents_total)/100);
    buff += "<tr><th align='left'>Job completion</th> <td align='right'>{0}%</td></tr>".format(Math.round(100.0*100.0*njobs_done/njobs_total)/100);
    buff += "</table>";
    $("#summary").html(buff);


}

function expandAll() {
    // do it this way because one guy may be reversed
    if(detailsVisible) {
        $("#toggle_all").text("expand all");
        $("[id^=details_]").slideUp(100);
    } else {
        $("#toggle_all").text("collapse all")
        $("[id^=details_]").slideDown(100);
    }
    detailsVisible = !detailsVisible;
}
