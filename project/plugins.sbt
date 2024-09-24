//// https://github.com/sbt/sbt-dynver
addSbtPlugin("com.github.sbt" % "sbt-dynver" % "5.0.1")

// https://github.com/sbt/sbt-native-packager
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.10.4")

// https://github.com/scalameta/sbt-scalafmt
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")

addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.7")

libraryDependencies += "com.thesamet.scalapb.zio-grpc" %% "zio-grpc-codegen" % "0.6.2"

// https://github.com/sbt/sbt-buildinfo
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.12.0")
