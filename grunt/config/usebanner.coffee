module.exports = {

  compiled_coffee:
    options:
      position: 'top'
      banner: '// This file has been generated from coffee source files\n'
    files:
      src: [ '<%= dir.target.js %>/**/*.js' ]


}
