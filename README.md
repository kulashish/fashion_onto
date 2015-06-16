# Alchemy
 DAP (Data & Analytics Platform) repository

### Compile, Test, and Coverage

Compile and create assembly jar (i.e., jar with all dependencies):

        $ cd core/
        $ sbt assembly

Run Unit Test

        $ sbt test

Run Code Coverage

        $ sbt clean coverage test
        $ sbt coverageReport

The coverage report will be available under target/scale-2.10/scoverage-report directory.
