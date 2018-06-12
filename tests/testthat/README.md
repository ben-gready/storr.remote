## storr.remote testing

### ssh

You will need `docker` installed and running and, the environment variable `STORR_REMOTE_USE_SSHD` set to the string `true`.  For example, add

```
STORR_REMOTE_USE_SSHD=true
```

to your `~/.Renviron`.  This will start a docker container called `storr-remote-sshd` that can be connected to using a set of ssh keys.  These keys will be copied to disk (in the `tests/testthat/sshd` directory).

With recent testthat, the `teardown-ssh.R` script will remove the container once tests have finished.

If the `ssh` package is not installed the server will not be started, as the tests cannot be run.
