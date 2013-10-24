module.exports = (grunt) ->

  grunt.registerTask 'showConfig', (prop)->
    console.log grunt.config.get(prop)
