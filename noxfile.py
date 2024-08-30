from __future__ import absolute_import

import nox

@nox.session
def unittests(session):
    session.install("-r", "requirements.txt")
    session.install("pytest")
    session.run("pytest", "-vvv", "--capture=no", "-rX", "--ignore", "evalbenchtest/*", success_codes=[0])