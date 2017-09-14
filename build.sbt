name := "castle-core"

organization := "com.box"

licenses += ("Apache 2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "com.box"                 %% "castle-router"          % "1.0.2",
  "com.box"                 %% "castle-committer-api"   % "1.0.1",
  "org.apache.kafka"        %% "kafka"                  % "0.8.1",
  "org.slf4s"               %% "slf4s-api"              % "1.7.13",
  "joda-time"               %  "joda-time"              % "2.9.4",
  "org.apache.curator"      %  "curator-test"           % "2.11.1",
  "org.apache.curator"      %  "curator-recipes"        % "2.11.1",
  "org.apache.curator"      %  "curator-framework"      % "2.11.1",
  "org.json4s"              %% "json4s-ext"             % "3.2.8",
  "org.json4s"              %% "json4s-jackson"         % "3.2.8",
  "com.typesafe.akka"       %% "akka-testkit"           % "2.3.11"  % "test",
  "org.specs2"              %% "specs2"                 % "2.4.2"   % "test"
)

ivyXML :=
  <dependencies>
    <exclude org="org.slf4j" module="slf4j-log4j12"/>
    <exclude org="org.slf4j" module="slf4j-simple"/>
    <exclude org="org.slf4j" module="slf4j-nop"/>
    <exclude org="com.sun.jmx" module="jmxri"/>
    <exclude org="com.sun.jdmk" module="jmxtools"/>
    <exclude org="javax.jms" module="jms"/>
  </dependencies>


publishMavenStyle := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) {
    Some("snapshots" at nexus + "content/repositories/snapshots")
  } else {
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
  }
}

// externalResolvers := resolvers map { rs =>
//  Resolver.withDefaultResolvers(rs, mavenCentral = true)
// }

licenses += ("Castle License", url("https://github.com/Box-Castle/core/blob/master/LICENSE"))

publishArtifact in Test := false

pomIncludeRepository := { _ => false }

pomExtra := {
  <url>https://github.com/Box-Castle/core</url>
  <scm>
    <url>git@github.com:Box-Castle/core.git</url>
    <connection>scm:git:git@github.com:Box-Castle/core.git</connection>
  </scm>
  <developers>
    <developer>
      <id>denisgrenader</id>
      <name>Denis Grenader</name>
      <url>http://github.com/denisgrenader</url>
    </developer>
  </developers>
}
