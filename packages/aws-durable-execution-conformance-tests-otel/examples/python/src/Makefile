.PHONY: build-Otel1Success build-Otel2WaitResume build-Otel3Retry build-Otel4TerminalFailure

build-Otel1Success build-Otel2WaitResume build-Otel3Retry build-Otel4TerminalFailure:
	python3 -m pip install --upgrade \
		--target "$(ARTIFACTS_DIR)" \
		--platform manylinux2014_x86_64 \
		--implementation cp \
		--python-version 3.13 \
		--only-binary=:all: \
		-r requirements.txt
	cp ./*.py "$(ARTIFACTS_DIR)/"
