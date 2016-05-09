var alldata = {};
var jsonFile = "data.json";
var baseDir = '/home/users/namin/duck_80x/NtupleTools/AutoTwopler';
var refreshSecs = 10*60;
var detailsVisible = false;
var duckMode = false;
var adminMode = false;
var numLogLines = 0;

google.charts.load('current', {'packages':['corechart']});
// google.charts.setOnLoadCallback(function() {console.log("google loaded!");});
google.charts.setOnLoadCallback(drawChart);

$(function() {


    loadJSON();
    setInterval(loadJSON, refreshSecs*1000);

    handleDuckMode();
    handleOtherTwiki();
    handleSubmitButton();
    handleAdminMode();

    $( document ).tooltip({ track: true });


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
            $(".mainlogo").attr('src', 'images/crab.png');
            $("#container").css("background", "");
            $("#firstTitle").text("auto");
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


function drawChart() {

    var data_table = [ [
        'Time',
        'Finished',
        'Transfer',
        'Running',
        'Failed',
        'Idle',
    ] ];
    // console.log(alldata["time_stats"]);
    
    // data_table.push( [ new Date(1458116239), 100, 100, 100, 100, 100, ] );
        
    prevNjobs = 0;
    for (var itd = 0; itd < alldata["time_stats"].length; itd++) {
        var td = alldata["time_stats"][itd];
        if($.isEmptyObject(td[1])) continue;

        var njobs = 0;
        for(var key in td[1]) njobs += td[1][key];
        // console.log(njobs);

        if(njobs < prevNjobs) {
            prevNjobs = njobs;
            continue;
        } else {
            prevNjobs = njobs;
        }

        data_table.push( [
                new Date(td[0]*1000), // to ms
                td[1]["finished"] ,
                td[1]["transferred"]+td[1]["transferring"],
                td[1]["running"],
                td[1]["cooloff"]+td[1]["failed"],
                td[1]["unsubmitted"]+td[1]["idle"],
        ] );
    }

    console.log(data_table);
    var data = google.visualization.arrayToDataTable(data_table);
    var options_stacked = {
        isStacked: true,
        height: 350,
        width: 850,
        legend: {position: 'right'},
        vAxis: {minValue: 0}
    };
    var chart = new google.visualization.AreaChart(document.getElementById('chart_div'));
    console.log(chart);
    chart.draw(data, options_stacked);
}

function loadJSON() {
    // $.getJSON("http://uaf-6.t2.ucsd.edu/~namin/dump/test.json", function(data) { parseJson(data); });
    // WOW CAN PUT EXTERNAL URLS HERE MAN!
    $.getJSON(jsonFile, function(data) { 
        if(!("samples" in alldata) || (data["samples"].length != alldata["samples"].length)) {
            setUpDOM(data);
        }
        fillDOM(data); 
    });
}

function doTwiki(type) {
    $("#twikiTextarea").val("Loading...");
    $("#message").html("");

    var formObj = {};
    formObj["action"] = type;
    if(type == "update") {
        var donesamples = [];
        for(var i = 0; i < alldata["samples"].length; i++) {
            if(alldata["samples"][i]["type"] == "BABY") continue;
            
            donesamples.push( alldata["samples"][i] );
        }
        console.log(donesamples);
        formObj["samples"] = JSON.stringify(donesamples);
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


function getProgress(sample) {
    var type = sample["type"];
    var stat = sample["status"];
    var done = 0;
    var tot = 1;

    if (type == "CMS3") {
        if (stat == "new") return 0.0;
        else if (stat == "crab") {

            if("breakdown" in sample["crab"]) {
                done = sample["crab"]["breakdown"]["finished"];
                tot = sample["crab"]["njobs"];
                if(tot < 1) tot = 1;
            }
            return 0.0 + 65.0*(done/tot);

        } else if (stat == "postprocessing") {

            if("postprocessing" in sample) {
                done = sample["postprocessing"]["done"];
                tot = sample["postprocessing"]["total"];
            }
            return 68.0 + 30.0*(done/tot);

        } else if (stat == "done") return 100;
        else return -1.0;

    } else if(type == "BABY") {
        done = sample["baby"]["done"];
        tot = sample["baby"]["total"];
        return 100.0*done/tot;
    }

}

function doSendAction(type, isample) {
    var dataset = alldata["samples"][isample]["dataset"]
    var shortname = dataset.split("/")[1];
    console.log("action,isample: " + type + " " + isample);

    // FIXME uncomment before letting people use
    if (!confirm('Are you sure you want to do the action: ' + type)) return;
    // FIXME uncomment before letting people use

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
    for(var i = 0; i < data["samples"].length; i++) {
        var sample = data["samples"][i];
        var toappend = "";
        toappend += "<br>";
        toappend += "<a href='#/' class='thick' onClick=\"$('#details_"+i+"').slideToggle(100)\">";
        if(sample["type"] == "BABY") {
            toappend += "<span style='color: purple'>[&#128700; "+sample["baby"]["analysis"]+" "+sample["baby"]["baby_tag"]+"]</span> ";
        } else if (sample["type"] == "CMS3") {
            toappend += "<span style='color: purple'>[CMS3]</span> ";
        }
        toappend += sample["dataset"]+"</a>";
        toappend += "<div class='pbar' id='pbar_"+i+"'>";
        toappend +=      "<span id='pbartextleft_"+i+"' class='pbartextleft'></span>";
        toappend +=      "<span id='pbartextright_"+i+"' class='pbartextright'></span>";
        toappend += "</div>";
        toappend += "<div id='details_"+i+"' style='display:none;'></div>";

        container.append(toappend);

        $( "#pbar_"+i ).progressbar({max: 100});
        $("#pbar_"+i).progressbar("option","value",0);
    }
}

function fillDOM(data) {
    alldata = data;

    var date = new Date(data["last_updated"]*1000); // ms to s
    var hours = date.getHours();
    var minutes = "0" + date.getMinutes();
    var seconds = "0" + date.getSeconds();
    var formattedTime = hours + ':' + minutes.substr(-2) + ':' + seconds.substr(-2);
    $("#last_updated").text("Last updated at " + date.toLocaleTimeString() + " on " + date.toLocaleDateString());

    for(var i = 0; i < data["samples"].length; i++) {
        var sample = data["samples"][i];

        var pct = Math.round(getProgress(sample));
        var color = 'hsl(' + pct*0.8 + ', 70%, 50%)';
        if(duckMode) {
            color = 'hsl(' + (pct*0.8+50) + ', 70%, 50%)';
        }
        if(pct == 100) {
            // different color if completely done
            color = 'hsl(' + pct*1.2 + ', 70%, 50%)';
        }

        $("#pbar_"+i).progressbar("value", pct);
        $("#pbar_"+i).find(".ui-progressbar-value").css({"background": color});
        var towrite = sample["status"] + " [" + pct + "%]";
        if(("crab" in sample) && ("status" in sample["crab"])) {
            if(sample["crab"]["status"] == "SUBMITFAILED") {
                towrite = "<span class='alert'>submit failed</span>";
            }
        }
        
        $("#pbartextright_"+i).html(towrite);
        $("#pbartextleft_"+i).html(""); 

        if(adminMode) {
            var buff = "";
            buff += "<a href='#/' onClick='doSendAction(\"kill\","+i+")' title='kill job (not enabled)'> &#9762; </a>  ";
            if(sample["type"] == "CMS3") {
                buff += "<a href='#/' onClick='doSendAction(\"skip_tail\","+i+")' title='skip tail CRAB jobs'> &#9986; </a> ";
                buff += "<a href='#/' onClick='doSendAction(\"repostprocess\","+i+")' title='re-postprocess'> &#128296; </a> ";
            }
            buff += "<a href='#/' onClick='doSendAction(\"email_done\","+i+")' title='send email when done'> &#9993; </a> ";

            $("#pbartextleft_"+i).html(buff);
        }

        var jsStr = syntaxHighlight(JSON.stringify(sample, undefined, 4));

        // turn crab into a link to the dashboard
        if(("crab" in sample) && ("uniquerequestname" in sample["crab"])) {
            var urn = sample["crab"]["uniquerequestname"];
            var link = "http://dashb-cms-job.cern.ch/dashboard/templates/task-analysis/#user=default&refresh=0&table=Jobs&status=&site=&tid="+urn;
            jsStr = jsStr.replace("\"crab\":", " <a href='"+link+"' style='text-decoration: underline'>crab</a>: ");
        }
        
        // bold the output directory and event counts if it's done
        if(("finaldir" in sample) && (sample["status"] == "done")) {
            jsStr = jsStr.replace("\"finaldir\":</span> <span class=\"string\">", "\"finaldir\":</span> <span class=\"string bold\">");
            jsStr = jsStr.replace("\"nevents_DAS\":</span> <span class=\"number\">", "\"nevents_DAS\":</span> <span class=\"number bold\">");
            jsStr = jsStr.replace("\"nevents_unmerged\":</span> <span class=\"number\">", "\"nevents_unmerged\":</span> <span class=\"number bold\">");
            jsStr = jsStr.replace("\"nevents_merged\":</span> <span class=\"number\">", "\"nevents_merged\":</span> <span class=\"number bold\">");
        }

        // turn dataset into a link to DAS
        jsStr = jsStr.replace("\"dataset\":", " <a href='https://cmsweb.cern.ch/das/request?view=list&limit=50&instance=prod%2Fglobal&input="+sample["dataset"]+"' style='text-decoration: underline'>dataset</a>: ");
        $("#details_"+i).html("<pre>" + jsStr + "</pre>");

    }

    var buff = "";
    if("log" in data) {
        var lines = data["log"].split("\n");
        for(var iline = 0; iline < lines.length; iline++) {
            var line = lines[iline];

            line = line.replace(/(\[[0-9\-\ \:\,]+\])/, '<span class="date_pfx">$1</span>');
            line = line.replace(/(\[[0-9A-Za-z\_\.\-\:\,]+\])/, '<span class="sample_pfx">$1</span>');
            
            buff += line + "\n";
        }
        $("#log_div").html("<br><pre class='logPane'>"+buff+"</pre>");
        console.log(lines.length);
        console.log(numLogLines);
        if(lines.length != numLogLines) {
            $(".logPane").animate({ scrollTop: $('.logPane').height()}, 1000);
            numLogLines = lines.length;
        } else {
            $(".logPane").scrollTop($('.logPane').height());
        }
    }


    // drawChart();
}

function expandAll() {
    // do it this way because one guy may be reversed
    if(detailsVisible) {
        $("#toggle_all").text("show details");
        $("[id^=details_]").slideUp(100);
    } else {
        $("#toggle_all").text("hide details")
        $("[id^=details_]").slideDown(100);
    }
    detailsVisible = !detailsVisible;
}
