li3\_gearman offers integration between [the most RAD PHP framework] [lithium]
and an excelent framework to farm out work to machines kown as [Gearman] [gearman]

# License #

li3\_gearman is released under the [BSD License] [license].

# Installation #

It is recommended that you install li3\_doctrine2 as a GIT submodule, in order
to keep up with the latest upgrades. To do so, switch to the core directory
holding your lithium application, and do:

```bash
$ git submodule add https://github.com/mariano/li3_gearman.git libraries/li3_gearman
```

You obviously need a Gearman server running somewhere.

# Usage #

There are two elements to li3\_gearman: the daemon (a lithium console command
named gearmand), and the utility class used to run/schedule work. Both tools
require you to define a configuration, where you specify which Gearman servers
are available. You do so by using the `config()` method of the `Gearman`
class somewhere in your boostrap process. For example, to define a `default`
configuration that uses a Gearman server located in th same server as your
lithium application, add the following code to 
`app/config/bootstrap/connections.php`:

```php
\li3_gearman\Gearman::config(array(
    'default' => array(
        'servers' => '127.0.0.1'
    )
));
```

Once you have a valid configuration, you can start the daemon, and start
scheduling / running jobs.

## Running the daemon ##

The daemon is a lithium console command called `gearmand`. Running it without
arguments will show a message similar to this:

```text
USAGE
    li3 gearmand start
    li3 gearmand stop
    li3 gearmand restart
DESCRIPTION
    Gearman daemon implementation in Lithium.
OPTIONS
    start
        Start the daemon.
    stop
        Stop the daemon. Only applicable if started in daemon mode.
    restart
        Restart the daemon. Only applicable if started in daemon mode.
    --blocking
        Enable to interact with Gearman in blocking mode. Default: disabled
    --daemon
        Enable to start daemon in a new process. Default: disabled
    --limit=<int>
        How many workers (in total) are allowed to be spawned before finishing
        daemon. Set to 0 to not limit spawned worker count. Default: 8
    --pid=<string>
        Location of PID file. Only applicable if daemon mode is enabled.
        Default: /var/run/li3_gearman.pid
    --resucitate
        If enabled, there will always be the number of workers defined in the
        setting "workers". If a worker dies, another one will take its place,
        up until the "limit" setting is reached. If disabled, no new
        workers will be spawned after the initial set is started.
        Default: disabled
    --verbose
        Enable to print out debug messages. If not enabled, messages go to
        user's syslog (usually /var/log/user.log). Default: disabled
    --workers=<int>
        How many workers to run. Default: 4
```

The daemon can be run in full daemon mode (meaning that a new process, detached
from the console, is created to handle the workers), or in interactive mode.

Whatever method you decide to use, note that a certain amount of workers
(processes) are spawned. This number is governed by the option `--workers`. If
you wish to ensure that there is always a certain number of workers active
and handling jobs, use the option `--resucitate`, which will periodically loop
through the pool to respawn workers whenever other workers are finished, for 
any reason. You can also limit this resucitation with the option `--limit`.

## Triggering jobs ##

Jobs can be triggered using the `Gearman::run()` method. This method takes up
to four arguments:

* `configName`: The configuration name to use (see *Usage* section above)
* `action`: What action to execute. If using the default `Job` adapter that
comes bundled with li3\_gearman, then this should be a fully qualified method.
This mean that it should consist of a fully qualified class name (with
namespaces), and a method name. If no method name is provided, the `run()`
method of the given class is assumed. Examples:

    * `app\tasks\Email::send`
    * `app\tasks\Caching`

[lithium]: http://lithify.me
[gearman]: http://gearman.org
[license]: http://www.opensource.org/licenses/bsd-license.php
