# Development Requiments #

The current development environment has been tested on linux ubuntu. If you are using another envionrment such as Mac or Windows, you will be responsible for any additional steps to get the application deployed.

To develop you will need to have the following components install:

1. JDK 1.8
2. Maven
3. Eclipse or IntelliJ
4. Postgresql 9.4(the other version doesn't work)
5. rabbitmq
6. mongodb
7. A custom elasticsearch 5.3.1(You should find it in resources/dataparse-elasticsearch.zip when you checkout the code)
8. nodejs and npm
9. git client

(Note: if you are using a Windows environment, you will need to have cygwin to emulate the bash environment.)

PS: If you do not know how to install the above components, you are most likely not qualified to work on 
the project. So I won't go into detail how to install and check to make sure those component function
as expected. You can find many documents and tutorial on the web. 

# Working With The Code #

To checkout the code

```
git clone git@github.com:david542542/datadocs_test.git
```

To modify/update the code you need to fork the project, update/modify the forked project. Once you complete the work and well tested, create a pull request. You can find more information at https://help.github.com/articles/about-pull-requests/ 

The current project code is organized as follow

```
datadocs
  module
    dataparse
  webui
  release
    app
    system
  resources
  docs
```

Where:

1. datadocs is the parent project and it has many subprojects
2. module/dataparse contain the backend java code and various test and related resources
3. webui contain the front end javascript, css, html ... code
4. release has 2 subprojects:
  - app contains script to build the dependecy projects, create the app release package and run the 
  application in local mode
  - system contains script that create vm server, install, deploy the required services on the 
  appropriated vm and run the application in cluster mode.
5. resources contains the various resources that use with the project such sql script to create db, user...
6. docs contains the documentation relates to the project

## Build The Code ##

For the first time, go to the datadocs_test run

```
mvn clean install -Dmaven.test.skip=true
```

You may need to open the pom.xml and comment out the modules section, run mvn clean install... 
uncomment the modules section and run mvn clean install again.  This is a problem with maven 
and there is no better way to fix.

Run the initdb.sql to create the database and testuser

```
sudo -u postgres psql -f resources/postgresql/initdb.sql
```

In each subproject module/dataparse, webui, release/app, release/system you will find a dev-tool.sh,
just run

```
./dev-tool.sh
```

It will show you the subcommand and instruction how to work with each subproject. For example, you will
get the following instructions if you run release/app/dev-tool.sh

```

Usage: 
  ./dev-tool.sh clean         To clean target and the other resources that the build
                              or test can produce
  ./dev-tool.sh run           To build the dependencies, create the release app and run
                              the application. Need to make sure that the postgresql, rabbitmq,
                              mongodb and elasticsearch are running
  ./dev-tool.sh build         To build the dependencies and jar files
  ./dev-tool.sh package       To compile , build and create app release directory

```

To run the project:

1. Make sure that you have postgresql, rabbitmq, mongodb running
2. Go to elasticsearch directory, launch elasticsearch with the command

        rm -rf logs data && ./bin/elasticsearch

3. Go to release/app and run:

        ./dev-tool.sh clean && ./dev-tool.run
        
    If not thing go wrong, the application should launch and you can access the webui via http://localhost:9100. You can now login with y/y for username/password

(You may need to change postgres, mongodb, elasticsearch port in the server.sh file according to your installation)

## To Develop The Backend ##

Make sure that the postgresql, rabbitmq, mongodb and elasticsearch running

Go to datadocs_test, generate the eclipse project configuration

```
mvn eclipse:eclipse -DdownloadSources=true
```

Open your eclipse and import the projects. Try to modify or insert some debug code into any 
classes in module/dataparse. 

To see your change with test:

```
./dev-tool.sh test 
```

To see your change with the running application, go to release app, run:

```
./dev-tool.sh run
```

The build, deploy and run process should take about 1 - 1:30 min

## To Develop The Frontend ##

You need to install nodejs and npm. You need to install gulp and gulp-cli as well.

Go to webui, run

```
npm install
```

To launch the webproxy server

```
./dev-tool.sh run-dev
```

This command will launch a web proxy server and watch for the modification in src directory. You can access
the webui now via  http://localhost:8283

Try to modify or insert some debug code into a javscript in src, you will see the the webui rebuild and deploy
automatically. The rebuild and deploy process should take less than 30s

You can use dev-tool.sh deploy, this command will copy the build to release/app/src/main/webapp
