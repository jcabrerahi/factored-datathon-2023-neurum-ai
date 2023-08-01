`Factored datathon 2023`_
=========

.. image:: https://github.com/jcabrerahi/factored-datathon-2023-neurum-ai/actions/workflows/ci_pipeline.yml/badge.svg?branch=develop
    :target: https://github.com/pylint-dev/pylint/actions

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
    :target: https://github.com/ambv/black

.. image:: https://img.shields.io/badge/linting-pylint-yellowgreen
    :target: https://github.com/pylint-dev/pylint

.. image:: https://raw.githubusercontent.com/jcabrerahi/factored-datathon-2023-neurum-ai/develop/badges/tests.svg
    :alt: Tests
    :target: https://github.com/jcabrerahi/factored-datathon-2023-neurum-ai

.. image:: https://raw.githubusercontent.com/jcabrerahi/factored-datathon-2023-neurum-ai/develop/badges/coverage.svg
    :alt: Coverage
    :target: https://github.com/jcabrerahi/factored-datathon-2023-neurum-ai


Project summary?
---------------

... is a `static code analyser`_ for Python 2 or 3. The latest version supports Python
3.8.0 and above.

.. _`static code analyser`: https://en.wikipedia.org/wiki/Static_code_analysis

... analyses your code without actually running it. It checks for errors, enforces a
coding standard, looks for `code smells`_, and can make suggestions about how the code
could be refactored.

.. _`code smells`: https://martinfowler.com/bliki/CodeSmell.html

Install
-------

.. This is used inside the doc to recover the start of the short text for installation

For command line use, pylint is installed with::

    pip install pylint

Or if you want to also check spelling with ``enchant`` (you might need to
`install the enchant C library <https://pyenchant.github.io/pyenchant/install.html#installing-the-enchant-c-library>`_):

.. code-block:: sh

    pip install pylint[spelling]

It can also be integrated in most editors or IDEs. More information can be found
`in the documentation`_.

.. _in the documentation: https://pylint.readthedocs.io/en/latest/user_guide/installation/index.html

.. This is used inside the doc to recover the end of the short text for installation


How to run the project
-----------------

Pylint isn't smarter than you: it may warn you about things that you have
conscientiously done or check for some things that you don't care about.
During adoption, especially in a legacy project where pylint was never enforced,
it's best to start with the ``--errors-only`` flag, then disable
convention and refactor messages with ``--disable=C,R`` and progressively
re-evaluate and re-enable messages as your priorities evolve.

License
-------

MIT
