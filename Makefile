.PHONY: build test

configure:
	pip install --upgrade pip
	pip install poetry

build:
	poetry install

update:
	poetry update

test:
	poetry run pytest -vv

help:
	@make info

clean:
	rm -rf *.egg-info

publish:
	poetry run publish-to-pypi

publish-for-ga:
	python -m pip install dcicutils==7.5.0
	poetry run publish-to-pypi --noconfirm

info:
	@: $(info Here are some 'make' options:)
	   $(info - Use 'make configure' to configure the repo by installing poetry.)
	   $(info - Use 'make build' to install entry point commands.)
	   $(info - Use 'make update' to update dependencies (and the lock file).)
	   $(info - Use 'make test' to run tests.)
