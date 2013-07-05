test: generate_test_report pyflakes
	python-coverage report -m

pyflakes:
	pyflakes base.py

run_test:
	python-coverage run --source=base test.py

generate_test_report: run_test
	rm -rf htmlcov
	python-coverage html
	
