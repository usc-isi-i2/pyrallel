Installation
============

.. note::

    Paralyzer only supports Python 3 and it's tested under Python 3.3+.

pip
----

Using pip to install::

    pip install paralyzer

If you want to update installed Paralyzer::

    pip install -U paralyzer

Development installation
------------------------

Install from source
```````````````````

::

    git clone https://github.com/usc-isi-i2/paralyzer.git
    cd paralyzer

    virtualenv paralyzer_env
    source activate paralyzer_env
    pip install -r requirements.txt
    pip install -r requreiments-dev.txt
    pip install -e .

Run tests
`````````

Paralyzer uses `pytest <https://pytest.org/>`_ for unit tests. To run them, simply do following command from the root of paralyzer package::

    pytest

If you need more detailed information, do::

    pytest -v --color=yes

Build documentation
-------------------

Additional dependencies for building documentation should be installed first::

    pip install -r requirements-docs.txt

Documentation is powered by `Sphinx <http://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html>`_ , to generate it on your local, please run::

    cd docs
    make html # the generated doc is located at _build/html/index.html
