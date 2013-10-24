module.exports = {

  compile:
    options:
      bare: true
      sourceMap: true
    files: [
      expand: true
      flatten: false
      cwd: '<%= dir.source.coffee %>'
      src: ['**/*.coffee']
      dest: '<%= dir.target.js %>'
      ext: '.js'
    ]


}
