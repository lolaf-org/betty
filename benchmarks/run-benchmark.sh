DATE=$(date +%Y-%m-%d)
if [ -n "$JAVA_HOME" ]; then
  JAVA="$JAVA_HOME/bin/java"
else
  JAVA="java"
fi
for BENCHMARK in ClientRTTBenchmark ClientThroughputBenchmark; do
  "$JAVA" -jar target/benchmarks.jar -rf json -prof gc "$BENCHMARK"
  mv jmh-result.json "results/jmh-result-${BENCHMARK}-${DATE}.json"
done
