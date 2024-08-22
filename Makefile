CHPL = chpl
CHPLFLAGS = -M src/ --fast

BENCHMARK_SOURCES = $(wildcard benchmarks/*.chpl)
BENCHMARK_EXECUTABLES = $(BENCHMARK_SOURCES:.chpl=)

%: %.chpl
	$(CHPL) $(CHPLFLAGS) $< -o $@

test:
	start_test tests/

benchmark: $(BENCHMARK_EXECUTABLES)

benchmark_clean:
	rm ${BENCHMARK_EXECUTABLES}
	rm benchmarks/*_real