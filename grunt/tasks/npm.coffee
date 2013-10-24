shell = require 'shelljs'

runSilent = (cmd) ->
  shell.exec cmd, silent: true

run = (cmd) ->
  shell.exec cmd

runSimple = (cmd) ->
  res = runSilent cmd
  return false unless res.code == 0


module.exports = (grunt) ->

  grunt.registerTask 'npm:publish', (tag) ->
    tag = tag || ''
    tag = ' -- tag ' + tag if tag
    return runSimple "npm publish #{tag}"
