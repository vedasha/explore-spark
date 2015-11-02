'use strict';
<<<<<<< 98ead9f5509191418b38d7397faad85afb5292a9
<<<<<<< feda269172f5ba0378296f440e023ebe36f9ccfe
=======
>>>>>>> add more rotate
Handlebars.registerHelper('step', function (data) {
    var ret = '';
    for (var key in data) {
        ret = ret + ' data-' + key + '="' + data[key] + '"';
<<<<<<< 98ead9f5509191418b38d7397faad85afb5292a9
    }
    return ret;
});

var appendSlides = function (data) {

    var steps = data;
    var htmltemplate = $('#step-template').html();
    var htmltempl = Handlebars.compile(htmltemplate);
    steps.forEach(function (step, index) {
        var templ = htmltempl;
        console.log(step);
        $.ajax({
            url: 'steps/' + step.uri,
            success: function (data) {
                $('.steps').append(templ({file: data, data: step.data,
                                          class: step.class, id: step.id}));
            },
            async: false
        });
    });
};

=======

var app = angular.module('app', []);


app.directive('onFinishRender', function($timeout) {
    return {
        restrict: 'A',
        link: function(scope, element, attr) {
            if (scope.$last === true) {
                $timeout(function() {
                    impress().init();
                });
            }
        }
=======
>>>>>>> add more rotate
    }
    return ret;
});

var appendSlides = function (data) {

    var steps = data;
    var htmltemplate = $('#step-template').html();
    var htmltempl = Handlebars.compile(htmltemplate);
    steps.forEach(function (step, index) {
        var templ = htmltempl;
        console.log(step);
        $.ajax({
            url: 'steps/' + step.uri,
            success: function (data) {
                $('.steps').append(templ({file: data, data: step.data,
                                          class: step.class, id: step.id}));
            },
            async: false
        });
    });
};

<<<<<<< 98ead9f5509191418b38d7397faad85afb5292a9
}])
>>>>>>> add impress effect
=======
>>>>>>> add more rotate
