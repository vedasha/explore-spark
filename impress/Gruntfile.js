// Generated on 2015-11-03 using generator-impress 0.1.2
'use strict';

<<<<<<< feda269172f5ba0378296f440e023ebe36f9ccfe
var LIVERELOAD_PORT = 35728;
=======
var LIVERELOAD_PORT = 35729;
>>>>>>> add impress effect
var lrSnippet = require('connect-livereload')({port: LIVERELOAD_PORT});
var mountFolder = function (connect, dir) {
    return connect.static(require('path').resolve(dir));
};

module.exports = function (grunt) {
    // load all grunt tasks
    require('matchdep').filterDev('grunt-*').forEach(grunt.loadNpmTasks);

    grunt.initConfig({
        watch: {
            options: {
                nospawn: true,
                livereload: LIVERELOAD_PORT
            },
            livereload: {
                files: [
                    'index.html',
                    'js/*.js',
                    'css/*.css',
                    'steps/*.html',
                    'steps/list.json'
                ]
            }
        },
        connect: {
            options: {
<<<<<<< feda269172f5ba0378296f440e023ebe36f9ccfe
                port: 9001,
=======
                port: 9000,
>>>>>>> add impress effect
                // change this to '0.0.0.0' to access the server from outside
                hostname: 'localhost'
            },
            livereload: {
                options: {
                    middleware: function (connect) {
                        return [
                            lrSnippet,
                            mountFolder(connect, '.')
                        ];
                    }
                }
            }
        },
        open: {
            server: {
                path: 'http://localhost:<%= connect.options.port %>'
            }
        }
    });

    grunt.registerTask('server', ['connect:livereload', 'open', 'watch']);
};
