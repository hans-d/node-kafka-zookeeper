loadConfig = (path) ->
  glob = require 'glob'
  config = {}

  glob.sync('*', cwd: path).forEach (option) ->
    key = option.replace /\.coffee$/,''
    config[key] = require path + option

  return config


module.exports = (grunt) ->

  # config
  config =
    pkgFile : 'package.json'
    pkg: grunt.file.readJSON 'package.json'
    dir:
      source:
        coffee: 'coffee'
      target:
          js: 'js'

  grunt.util._.extend config, loadConfig './grunt/config/'
  grunt.initConfig config


  # task definitions
  require('load-grunt-tasks')(grunt)
  grunt.loadTasks './grunt/tasks'


  # task aliasses
  grunt.registerTask 'default', [ 'test', 'build' ]

  grunt.registerTask 'test', [ 'mochaTest:specDot' ]
  grunt.registerTask 'test:spec', [ 'mochaTest:specSpec' ]
  grunt.registerTask 'test:coverage', [ 'mochaTest:unitJS', 'mochaTest:coverageJS' ]

  grunt.registerTask 'build', [ 'clean', 'compile' ]
  grunt.registerTask 'compile', [ 'compile:coffee' ]
  grunt.registerTask 'compile:coffee', [ 'coffee', 'usebanner:compiled_coffee' ]

  grunt.registerTask 'release:prep', [ 'git:isClean', 'semver:isSynced', 'build', 'test', 'git:isClean' ]
  grunt.registerTask 'release:commit', [ 'git:add:package.json', 'git:commit:release', 'git:tag:release' ]

  grunt.registerTask 'release:patch', [ 'release:prep', 'semver:bump:patch', 'release:commit' ]
  grunt.registerTask 'release:minor', [ 'release:prep', 'semver:bump:minor', 'release:commit' ]
  grunt.registerTask 'release:major', [ 'release:prep', 'semver:bump:major', 'release:commit' ]
  grunt.registerTask 'release:nobump', [ 'release:prep', 'git:tag:release' ]

  grunt.registerTask 'release:push', ['git:push', 'git:pushTags' ]
  grunt.registerTask 'release:publish', ['release:push', 'npm:publish' ]

