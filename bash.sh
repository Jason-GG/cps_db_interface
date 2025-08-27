

pip install twine
pip install --upgrade setuptools wheel build


rm -rf dist build *.egg-info  # clean old builds
python -m build

twine upload --repository-url https://aws:$CODEARTIFACT_AUTH_TOKEN@cps-mstr-cloud-570188313908.d.codeartifact.us-east-1.amazonaws.com/pypi/cps-pip/ dist/*
