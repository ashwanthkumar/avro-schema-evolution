
name := "AvroSchemaEvolution"

version := "0.1"

scalaVersion := "2.11.8"

(javaSource in AvroConfig) := baseDirectory.value / "src/main/gen-java/"
(stringType in AvroConfig) := "String"