logLevel := sbt.Level.Warn
addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.6.0")
addSbtPlugin("com.typesafe.sbt" % "sbt-multi-jvm" % "0.3.8")
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.1.0")
addSbtPlugin("org.scoverage" %% "sbt-scoverage" % "1.5.0")
addSbtPlugin("org.scoverage" %% "sbt-coveralls" % "1.1.0")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.7.4")
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.8.0")

