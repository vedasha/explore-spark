'use strict';

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
    }
});

app.controller('controller', ['$scope', function($scope) {

    var coordinateShift = 4000;
    var rotateShift = 90;
    var scaleShift = 4;

    var shiftX = [
        ["x", "+", coordinateShift, "move right"],
        ["x", "-", coordinateShift, "move left"]
    ];
    var shiftY = [
        ["y", "+", coordinateShift, "move down"],
        ["y", "-", coordinateShift, "move up"]
    ];
    var shiftZ = [
        ["z", "+", coordinateShift*2, "zoom out"],
        ["z", "-", coordinateShift*2, "zoom in"]
    ];

    var rotateX = [
        ["rotateX", "=", rotateShift, "rotate x"],
        ["rotateX", "=", -rotateShift, "rotate y"]
    ];
    var rotateY = [
        ["rotateY", "=", rotateShift, "rotate y"],
        ["rotateY", "=", -rotateShift, "rotate y"]
    ];
    var rotateZ = [
        ["rotateZ", "=", rotateShift, "rotate z"],
        ["rotateZ", "=", -rotateShift, "rotate z"]
    ];

    var scale = [
        ["scale", "*", scaleShift],
        ["scale", "/", scaleShift]
    ];

    var transforms = [shiftX, shiftY, shiftZ, rotateX, rotateY, rotateZ, scale];

    function cartesian() {
        var r = [],
            arg = arguments,
            max = arg.length - 1;

        function helper(arr, i) {
            for (var j = 0, l = arg[i].length; j < l; j++) {
                var a = arr.slice(0); // clone arr
                a.push(arg[i][j]);
                if (i == max)
                    r.push(a);
                else
                    helper(a, i + 1);
            }
        }
        helper([], 0);
        return r;
    }

    function getCombinations(arr, n) {
        var i, j, k, elem, l = arr.length,
            childperm, ret = [];
        if (n == 1) {
            for (var i = 0; i < arr.length; i++) {
                for (var j = 0; j < arr[i].length; j++) {
                    ret.push([arr[i][j]]);
                }
            }
            return ret;
        } else {
            for (i = 0; i < l; i++) {
                elem = arr.shift();
                for (j = 0; j < elem.length; j++) {
                    childperm = getCombinations(arr.slice(), n - 1);
                    for (k = 0; k < childperm.length; k++) {
                        ret.push([elem[j]].concat(childperm[k]));
                    }
                }
            }
            return ret;
        }
        // i=j=k=elem=l=childperm=ret=[]=null;
    }
    var get = function(param, actions) {
        var i;
        for (i in actions) {
            var action = actions[i];
            var op = action[1];
            var shift = action[2];
            if (action[0] === param) {
                if (op === '+') {
                    $scope[param] += shift
                } else if (op === '-') {
                    $scope[param] -= shift
                } else if (op === '*') {
                    $scope[param] *= shift
                } else if (op === '/') {
                    $scope[param] /= shift
                } else if (op === '=') {
                    $scope[param] = shift
                }
            }
        }
        return $scope[param];
    }
    var getX = function(actions) {
        return get('x', actions);
    }
    var getY = function(actions) {
        return get('y', actions);
    }
    var getZ = function(actions) {
        return get('z', actions);
    }

    function shuffle(array) {
        var currentIndex = array.length,
            temporaryValue, randomIndex;

        // While there remain elements to shuffle...
        while (0 !== currentIndex) {

            // Pick a remaining element...
            randomIndex = Math.floor(Math.random() * currentIndex);
            currentIndex -= 1;

            // And swap it with the current element.
            temporaryValue = array[currentIndex];
            array[currentIndex] = array[randomIndex];
            array[randomIndex] = temporaryValue;
        }

        return array;
    }

    $scope.x = 0;
    $scope.y = 0;
    $scope.z = 0;
    $scope.rotateX = 0;
    $scope.rotateY = 0;
    $scope.rotateZ = 0;
    $scope.scale = 4;

    var getActionDescriptions = function(actions) {
        var actionDescriptions = [];
        var i;
        for (i in actions) {
            var action = actions[i];
            var actionDescrption = action[3] + " " + action[2];
            actionDescriptions.push(actionDescrption);
        }
    }
    var actionsCombination1 = shuffle(getCombinations(
        [shiftX, rotateZ, rotateX, scale, shiftY, shiftZ, rotateY], 1));
    var getLocations = function() {
        var locations = [];
        var i;

        var append = function(actions) {
            var location = {
                "x": get('x', actions),
                "y": get('y', actions),
                "z": get('z', actions),
                "rotateX": get('rotateX', actions),
                "rotateY": get('rotateY', actions),
                "rotateZ": get('rotateZ', actions),
                "scale": get('scale', actions),
                "actions": getActionDescriptions(actions)
            };
            locations.push(location)
            console.log(location);
        }
        for (i = 0; i < actionsCombination1.length; i += 4) {
            append(actionsCombination1[i])
        }
        for (i = 1; i < actionsCombination1.length; i += 4) {
            append(actionsCombination1[i])
        }
        for (i = 2; i < actionsCombination1.length; i += 4) {
            append(actionsCombination1[i])
        }
        for (i = 3; i < actionsCombination1.length; i += 4) {
            append(actionsCombination1[i])
        }
        return locations;

    };
    $scope.locations1 = getLocations();

    $scope.actionsCombination2 = getCombinations(
        [shiftX, shiftY, shiftZ, rotateX, rotateY, rotateZ, scale], 2);
    $scope.actionsCombination3 = getCombinations(
        [shiftX, shiftY, shiftZ, rotateX, rotateY, rotateZ, scale], 3);

}])
