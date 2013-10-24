shell = require 'shelljs'

runSilent = (cmd) ->
  shell.exec cmd, silent: true

runSimple = (cmd) ->
  res = runSilent cmd
  return false unless res.code == 0


module.exports = (grunt) ->

  grunt.registerTask 'git:add', (file) ->
    res = runSimple "git add #{file}"

  grunt.registerTask 'git:commit:release', ->
    msg = grunt.config 'release.options.commitMessage'
    return runSimple "git commit -m '#{msg}'"

  grunt.registerTask 'git:isClean', ->
    res = runSilent 'git status -s'
    return unless res.output
    grunt.log.error 'Uncommitted changes'
    grunt.log.writelns res.output
    return false

  grunt.registerTask 'git:push', ->
    return runSimple 'git push'

  grunt.registerTask 'git:pushTags', ->
    return runSimple 'git push --tags'

  grunt.registerTask 'git:tag:release', ->
    tag = grunt.config 'release.options.tagName'
    msg = grunt.config 'release.options.tagMessage'
    return runSimple "git tag  #{tag} -m '#{msg}'"
