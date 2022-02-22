********************************************************************************
Go Snowflake Driver
********************************************************************************

.. image:: https://github.com/snowflakedb/gosnowflake/workflows/Build%20and%20Test/badge.svg?branch=master
    :target: https://github.com/snowflakedb/gosnowflake/actions?query=workflow%3A%22Build+and+Test%22

.. image:: http://img.shields.io/:license-Apache%202-brightgreen.svg
    :target: http://www.apache.org/licenses/LICENSE-2.0.txt

.. image:: https://goreportcard.com/badge/github.com/snowflakedb/gosnowflake
    :target: https://goreportcard.com/report/github.com/snowflakedb/gosnowflake

This topic provides instructions for installing, running, and modifying the Go Snowflake Driver. The driver supports Go's `database/sql <https://golang.org/pkg/database/sql/>`_ package.

Prerequisites
================================================================================

The following software packages are required to use the Go Snowflake Driver.

Go
----------------------------------------------------------------------

The latest driver requires the `Go language <https://golang.org/>`_ 1.14 or higher. The supported operating systems are Linux, Mac OS, and Windows, but you may run the driver on other platforms if the Go language works correctly on those platforms.


Installation
================================================================================

Get Gosnowflake source code, if not installed.

.. code-block:: bash

    go get -u github.com/snowflakedb/gosnowflake

Docs
====

For detailed documentation and basic usage examples, please see the documentation at
`godoc.org <https://godoc.org/github.com/snowflakedb/gosnowflake/>`_.

Sample Programs
================================================================================

Snowflake provides a set of sample programs to test with. Set the environment variable ``$GOPATH`` to the top directory of your workspace, e.g., ``~/go`` and make certain to 
include ``$GOPATH/bin`` in the environment variable ``$PATH``. Run the ``make`` command to build all sample programs.

.. code-block:: go

    make install

In the following example, the program ``select1.go`` is built and installed in ``$GOPATH/bin`` and can be run from the command line:

.. code-block:: bash

    SNOWFLAKE_TEST_ACCOUNT=<your_account> \
    SNOWFLAKE_TEST_USER=<your_user> \
    SNOWFLAKE_TEST_PASSWORD=<your_password> \
    select1
    Congrats! You have successfully run SELECT 1 with Snowflake DB!

Development
================================================================================

The developer notes are hosted with the source code on `GitHub <https://github.com/snowflakedb/gosnowflake>`_.

Testing Code
----------------------------------------------------------------------

Set the Snowflake connection info in ``parameters.json``:

.. code-block:: json

    {
        "testconnection": {
            "SNOWFLAKE_TEST_USER":      "<your_user>",
            "SNOWFLAKE_TEST_PASSWORD":  "<your_password>",
            "SNOWFLAKE_TEST_ACCOUNT":   "<your_account>",
            "SNOWFLAKE_TEST_WAREHOUSE": "<your_warehouse>",
            "SNOWFLAKE_TEST_DATABASE":  "<your_database>",
            "SNOWFLAKE_TEST_SCHEMA":    "<your_schema>",
            "SNOWFLAKE_TEST_ROLE":      "<your_role>"
        }
    }

You can find the complete file in the [Sigma 1Password](https://my.1password.com/vaults/likk64vc3hl7iaozanwj3dn7vu/allitems/72eslwc2yrglsfadkepljc45ai)

Install `jq <https://stedolan.github.io/jq/>`_ so that the parameters can get parsed correctly, and run ``make test`` in your Go development environment:

.. code-block:: bash

    make test

Submitting Pull Requests
----------------------------------------------------------------------

You may use your preferred editor to edit the driver code. Make certain to run ``make fmt lint`` before submitting any pull request to Snowflake. This command formats your source code according to the standard Go style and detects any coding style issues.

Updating to Latest Upstream
----------------------------------------------------------------------

In general, we want to keep our fork up-to-date with changes from the `upstream gosnowflake repo <https://github.com/snowflakedb/gosnowflake>`_. Follow these steps to pull upstream changes into our fork.

First, add the upstream repo as a remote:

.. code-block:: bash

    git remote add upstream git@github.com:snowflakedb/gosnowflake.git
    git fetch upstream master

To see the number of commits in ``upstream/master`` but not in ``origin/master``:

.. code-block:: bash

    git log --oneline origin/master..upstream/master | wc -l

To see the number of commits in ``origin/master`` but not in ``upstream/master``:

.. code-block:: bash

    git log --oneline upstream/master..origin/master | wc -l

Next, we want to create a merge commit that incorporates changes from the upstream repo with changes in our fork. These steps are based on `a blog post about maintaining long-lived forks <https://die-antwort.eu/techblog/2016-08-git-tricks-for-maintaining-a-long-lived-fork/>`_.

.. code-block:: bash

    # Create a branch reset-to-upstream that is up-to-date with upstream.
    git checkout -b reset-to-upstream upstream/master

    # Merge master into this branch, fixing any conflicts
    git merge --strategy=ours master

    # Switch back to master and merge the temporary branch into a new branch
    # off of master (will be a fast-forward merge).
    git checkout master
    git checkout -b update-master-with-upstream
    git merge reset-to-upstream

    # Sanity check: verify this shows only _our_ changes
    git log --oneline upstream/master..HEAD

    # Fix any code breakage because of changes to types/includes etc.

**Note**: ensure that `go.mod` uses `sigmacomputing/gosnowflake` as its module name, and (2) references the correct upstream *version* for `snowflakedb/gosnowflake`

Now we can push the branch ``update-master-with-upstream`` to our fork's origin, and create a pull request.

Once that's done, we can run the Multiplex benchmarks with the new version of gosnowflake. In your local copy of the mono-go repo, update Multiplex to use the version of gosnowflake in ``update-master-with-upstream``:

.. code-block:: bash

    go get github.com/sigmacomputing/gosnowflake@<new gosnowflake commit hash>

Create a commit with this update, along with any fixes to get Multiplex to work with the latest driver version.

To run the benchmark, populate the file ``multiplex/scripts/parameters.json`` with Snowflake testing credentials, which can be found in the Sigma Engineering 1Password vault. Then, run the benchmark script to compare the old and new branches, and redirect output to a csv file.

.. code-block:: bash

    ./multiplex/scripts/query_bench.sh <old commit hash> <new commit hash> > ~/tmp/gosnowflake-bench-2021-06-11.csv

Once that's done running, add a new page to the `Snowflake Go Driver Updates workbook <https://staging.sigmacomputing.io/sigma-on-sigma/workbook/Snowflake-Go-Driver-Updates-Hm0VmWLJZbgi9m4hWayjK>`_ with the results from your benchmark run. If there were any errors while running any of the queries or there was a substantial regression in query runtime, investigate before merging. If everything looks good (all queries ran, median and p90 runtime are within a few percent), merge the commit into gosnowflake.

After the update is merged to gosnowflake, go back to the ``mono-go`` repo and run ``go get github.com/sigmacomputing/gosnowflake@master`` to update the version of gosnowflake that Multiplex uses. Create a PR for the upgrade, get feedback, and merge it into master.

Support
----------------------------------------------------------------------

For official support, contact Snowflake support at:
https://support.snowflake.net/

Setting up the CI credentials
----------------------------------------------------------------------

You shouldn't need to do this, but in case we need to rotate the CI credentials, here are the steps I followed to create them:

1. Install ``gpg`` if you don't already have it:

.. code-block:: bash

    brew install gpg

2. Get the `gpg passphrase <https://my.1password.com/vaults/likk64vc3hl7iaozanwj3dn7vu/allitems/esdnmyqh5c3cze3k67tqrkd5s4>`_ and the raw `parameters.json file <https://my.1password.com/vaults/likk64vc3hl7iaozanwj3dn7vu/allitems/72eslwc2yrglsfadkepljc45ai>`_ from the Sigma 1Password.

3. Use ``gpg``'s symmetric encryption mode to encrypt the ``parameters.json`` file. You'll be prompted twice to enter the passphrase:

.. code-block:: bash

    gpg --symmetric --cipher-algo AES256 --output .github/workflows/parameters_aws_golang.json.gpg parameters.json

4. Get the `TEST_USER private key <https://sigmacomputing.1password.com/vaults/likk64vc3hl7iaozanwj3dn7vu/allitems/7g4gv6wjbbh6bgt7t6v6dlbhke>`_ from the Sigma 1Password. The TEST_USER keypair secret includes a public key, an encrypted private key, and the passphrase used to encrypt the private key; copy only the encrypted private key into ``rsa-2048-private-key-enc.p8``)

5. Remove the passphrase from the private key (you'll be prompted for the private key passphrase), then use ``gpg``'s symmetric encryption mode to encrypt the resulting unencrypted private key (we only need one layer of encryption and it's easier to standardize on ``gpg``). As with the ``parameters.json`` file, you'll be prompted twice to enter the gpg passphrase:

.. code-block:: bash

    openssl pkcs8 -in rsa-2048-private-key-enc.p8 -out rsa-2048-private-key.p8
    gpg --symmetric --cipher-algo AES256 --output .github/workflows/rsa-2048-private-key.p8.gpg rsa-2048-private-key.p8

6. Ensure that the gpg passphrase is configured properly in the `GitHub Environment <https://docs.github.com/en/actions/reference/environments>`_
