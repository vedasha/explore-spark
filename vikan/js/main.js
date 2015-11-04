'use strict';
<<<<<<< 295e6c2a15848a54cb55190a1293d5ac5b14815a
<<<<<<< 092b1974372e705e5780b84cff4870e07c5d0f71
Handlebars.registerHelper('step', function(data) {
=======
Handlebars.registerHelper('step', function (data) {
>>>>>>> add vikan presentation
=======
Handlebars.registerHelper('step', function(data) {
>>>>>>> add poll
    var ret = '';
    for (var key in data) {
        ret = ret + ' data-' + key + '="' + data[key] + '"';
    }
    return ret;
});

<<<<<<< 295e6c2a15848a54cb55190a1293d5ac5b14815a
<<<<<<< 092b1974372e705e5780b84cff4870e07c5d0f71
var appendSlides = function(data) {
=======
var appendSlides = function (data) {
>>>>>>> add vikan presentation
=======
var appendSlides = function(data) {
>>>>>>> add poll

    var steps = data;
    var htmltemplate = $('#step-template').html();
    var htmltempl = Handlebars.compile(htmltemplate);
<<<<<<< 295e6c2a15848a54cb55190a1293d5ac5b14815a
<<<<<<< 092b1974372e705e5780b84cff4870e07c5d0f71
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
=======
    steps.forEach(function (step, index) {
=======
    steps.forEach(function(step, index) {
>>>>>>> add poll
        var templ = htmltempl;
        console.log(step);
        $.ajax({
            url: 'steps/' + step.uri,
<<<<<<< 295e6c2a15848a54cb55190a1293d5ac5b14815a
            success: function (data) {
                $('.steps').append(templ({file: data, data: step.data,
                                          class: step.class, id: step.id}));
>>>>>>> add vikan presentation
=======
            success: function(data) {
                $('.steps').append(templ({
                    file: data,
                    data: step.data,
                    class: step.class,
                    id: step.id
                }));
>>>>>>> add poll
            },
            async: false
        });
    });
};

<<<<<<< 295e6c2a15848a54cb55190a1293d5ac5b14815a
<<<<<<< 092b1974372e705e5780b84cff4870e07c5d0f71
=======
>>>>>>> add poll
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

<<<<<<< 295e6c2a15848a54cb55190a1293d5ac5b14815a
var updateGraph = function(votes, options, totalpolls) {
=======
var updateGraph = function(votes, options) {
>>>>>>> add poll
    var highestVote = 0;
    for (var i in votes) {
        if (votes[i] > highestVote) {
            highestVote = votes[i]
        }
    }

<<<<<<< 295e6c2a15848a54cb55190a1293d5ac5b14815a
    var rate = 400 / highestVote;
    // d3.select('#totalpolls').text(totalpolls);
=======
    var rate = 400 / highestVote
>>>>>>> add poll
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
<<<<<<< 295e6c2a15848a54cb55190a1293d5ac5b14815a
        updateGraph(votes, options, data.length);
    })
}
=======
>>>>>>> add vikan presentation
=======
        updateGraph(votes, options);
    })
}
>>>>>>> add poll
