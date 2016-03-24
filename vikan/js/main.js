'use strict';
Handlebars.registerHelper('step', function(data) {
    var ret = '';
    for (var key in data) {
        ret = ret + ' data-' + key + '="' + data[key] + '"';
    }
    return ret;
});

var appendSlides = function(data) {

    var steps = data;
    var htmltemplate = $('#step-template').html();
    var htmltempl = Handlebars.compile(htmltemplate);
    steps.forEach(function(step, index) {
        var templ = htmltempl;
        console.log(step);
        $.ajax({
            url: 'steps/' + step.uri,
            success: function(data) {
                $('.steps').append(templ({
                    file: data,
                    data: step.data,
                    class: step.class,
                    id: step.id
                }));
            },
            async: false
        });
    });
};

var c10 = d3.scale.category10();
var dbUrl = 'https://api.mongolab.com/api/1/databases/rockie_test';
var apiKey = 'Wq6LkbyEZjQJ5D-HY22jAH7XWM3T2M7z';
var optionsUrl = dbUrl + '/collections/options?apiKey=' + apiKey;
var votesUrl = dbUrl + '/collections/votes?apiKey=' + apiKey;

var purgeQuestion = function() {
    $.ajax({
        url: optionsUrl,
        data: JSON.stringify([]),
        type: "PUT",
        contentType: "application/json"
    });

}
var publishQuestion = function(options) {
    $.ajax({
        url: optionsUrl,
        data: JSON.stringify(options),
        type: "PUT",
        contentType: "application/json"
    });
}

var updateGraph = function(votes, options, totalpolls) {
    var highestVote = 0;
    for (var i in votes) {
        if (votes[i] > highestVote) {
            highestVote = votes[i]
        }
    }

    var rate = 400 / highestVote;
    // d3.select('#totalpolls').text(totalpolls);
    d3.select('#poll-result').selectAll("div")
        .data(votes).enter().append("div").attr("class", "bar").style("height", 0)
        .transition().duration(3000)
        .delay(function(d, i) {
            return i * 300;
        })
        // .text(function(d, i){
        //     return options[i].heading;
        // })
        .style("height", function(d) {
            var height = d * rate;
            height = height > 0 ? height : 3;
            return height + "px";
        })
        .style('background-color', function(d, i) {
            return c10(i);
        })
}
var fetchResult = function(options) {
    d3.json(votesUrl, function(error, data) {
        var votes = [0, 0, 0, 0, 0, 0];

        for (var i in data) {
            var vote = data[i].vote - 1;
            votes[vote] += 1;
        }

        console.log(votes);
        updateGraph(votes, options, data.length);
    })
}
