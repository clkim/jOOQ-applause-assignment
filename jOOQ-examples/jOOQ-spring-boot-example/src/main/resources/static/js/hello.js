angular.module('hello', [ 'ngRoute', 'isteven-multi-select' ])
  .config(function($routeProvider, $httpProvider) {

    $routeProvider.when('/', {
      templateUrl : 'home.html',
      controller : 'home',
      controllerAs: 'controller'
    }).when('/login', {
      templateUrl : 'login.html',
      controller : 'navigation',
      controllerAs: 'controller'
    }).otherwise('/');

    $httpProvider.defaults.headers.common["X-Requested-With"] = 'XMLHttpRequest';

  })
  .controller('home', function($http) {
      var self = this;
      $http.get('/resource/').then(function(response) {
        self.greeting = response.data;
      })
  })
  .controller('navigation', function($rootScope, $http, $location) {
      $rootScope.showresult = false;
      var self = this;
      $http.get('/allcountries/').then(function(response) {
          self.allcountries = response.data;
          var countriesArray = self.allcountries["countries"].split(',');
          var flagsArray = self.allcountries["flags"].split(',');
          // build the array of items to show in multi-select
          self.showCountries = new Array();
          for (i = 0; i < countriesArray.length; i++) {
              self.showCountries.push({
                  icon: flagsArray[i],
                  name: countriesArray[i],
                  ticked: countriesArray[i] == 'US' ? true : false});
          }
      })

      // called by submit button click listener below
      //  this is a bit of a hack to concatenate a query string at end
      //  TODO seems better to pass in named query request parameters https://spring.io/guides/gs/rest-service/
      var doMatch = function(namesCsv) {
        $http.get('/testers/'+namesCsv+'/').then(
            // success call-back
            function(response) {
                if (response.data.testers) {
                    $rootScope.showresult = true;
                    self.error = false;
                } else {
                    $rootScope.showresult = false;
                }
                self.testers = response.data.testers
            },
            // failure call-back
            function() {
                $rootScope.showresult = false;
                self.error = true;
            }
        );
      }

      // called on submit button click
      self.match = function() {
        var namesArray = self.selectedCountries.map(function(obj) {return obj['name']});
        doMatch(namesArray.join());
        $location.path("/login");
      };

      // called on clicking logout tab, mimic some action
      self.logout = function() {
        // just hide the Result section
        $rootScope.showresult = false;
        $location.path("/login");
      }
  });
