test: generate_test_report
	python-coverage report -h

run_test:
	python-coverage run --source=base test.py

generate_test_report: run_test
	rm -rf htmlcov
	python-coverage html -m
	
