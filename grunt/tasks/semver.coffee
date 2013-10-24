shell = require 'shelljs'

runSilent = (cmd) ->
  shell.exec cmd, silent: true


module.exports = (grunt) ->

  grunt.registerTask 'semver:bump', (type) ->
    type = type || 'patch'
    grunt.task.run 'bumpup:' + type

  grunt.registerTask 'semver:isSynced', ->
    res = runSilent "node_modules/.bin/semver-sync --verify"
    return false unless res.code == 0
    if res.code == 0
      grunt.log.writeln res.output
      return
    grunt.error res.output
    return false
