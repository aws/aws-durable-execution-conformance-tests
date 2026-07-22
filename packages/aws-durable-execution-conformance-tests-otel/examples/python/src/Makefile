.PHONY: build-Otel1Success build-Otel2WaitResume build-Otel3Retry build-Otel4TerminalFailure \
	build-Otel5ChildContext build-Otel6Parallel build-Otel7Map build-Otel8HandledFailure \
	build-Otel9WaitForCondition build-Otel10WaitForCallback build-Otel11ChainedInvoke \
	build-Otel11InvokeTarget build-Otel12ChildContextFailure build-Otel13ParallelFailure \
	build-Otel14MapFailure build-Otel15WaitInterrupted build-Otel16WaitForConditionFailure \
	build-Otel17WaitForCallbackFailure build-Otel18ChainedInvokeFailure \
	build-Otel18InvokeTarget build-Otel19ExecutionFailure

build-Otel1Success build-Otel2WaitResume build-Otel3Retry build-Otel4TerminalFailure \
build-Otel5ChildContext build-Otel6Parallel build-Otel7Map build-Otel8HandledFailure \
build-Otel9WaitForCondition build-Otel10WaitForCallback build-Otel11ChainedInvoke \
build-Otel11InvokeTarget build-Otel12ChildContextFailure build-Otel13ParallelFailure \
build-Otel14MapFailure build-Otel15WaitInterrupted build-Otel16WaitForConditionFailure \
build-Otel17WaitForCallbackFailure build-Otel18ChainedInvokeFailure \
build-Otel18InvokeTarget build-Otel19ExecutionFailure:
	python3 -m pip install --upgrade \
		--target "$(ARTIFACTS_DIR)" \
		--platform manylinux2014_x86_64 \
		--implementation cp \
		--python-version 3.13 \
		--only-binary=:all: \
		-r requirements.txt
	cp ./*.py "$(ARTIFACTS_DIR)/"
