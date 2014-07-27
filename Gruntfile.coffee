
module.exports = (grunt) ->

  opts =
  # Coffee-script tasks
    coffee:
      build:
        options:
          join: true
        files: [(
                  cwd: "src"
                  src: "*.coffee"
                  dest: "lib"
                  expand: true
                  ext: ".js"
                )]

  # Watcher tasks
    watch:
      options:
        atBegin: true
      coffee:
        files: ["src/*.coffee"]
        tasks: ["coffee:build"]

  grunt.initConfig(opts)

  grunt.loadNpmTasks("grunt-contrib-coffee")
  grunt.loadNpmTasks("grunt-contrib-watch")

  grunt.registerTask("default", ["watch"])
