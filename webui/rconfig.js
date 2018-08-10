(function(){
  var commonModuleDeps = [
      'angular', 'angular-ui-bootstrap-tpls', 'angular-storage', 'angular-sanitize',
      'jquery', 'jquery-ui', 'lodash', 'spin', 'ladda', 'alertify', 'utils'
  ];
  return {
    dir: 'build/js',
    modules: [
      { name: "main" },
      { name: "modules/login/module", exclude: commonModuleDeps },
      { name: "modules/main/module", exclude: commonModuleDeps }
    ],
    baseUrl: 'src/js',
    mainConfigFile: "src/js/main.js",
    findNestedDependencies: true,
    skipDirOptimize: true,
    keepBuildDir: true,
    removeCombined: true
  }
})();
