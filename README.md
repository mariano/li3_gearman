li3\_gearman offers integration between [the most RAD PHP framework] [lithium]
and an excelent framework to farm out work to machines known as 
[Gearman] [gearman].

# License #

li3\_gearman is released under the [BSD License] [license].

# Installation #

It is recommended that you install li3\_gearman as a GIT submodule, in order
to keep up with the latest upgrades. To do so, switch to the core directory
holding your lithium application, and do:

```bash
$ git submodule add https://github.com/mariano/li3_gearman.git libraries/li3_gearman
```

Once you have downloaded li3\_gearman and placed it in your main `libraries`
folder, or your `app/libraries` folder, you need to enable it by placing the 
following at the end of your `app/config/bootstrap/libraries.php` file:

```php
Libraries::add('li3_gearman');
```

You obviously need a Gearman server running somewhere.

# Usage #

There are two main elements to li3\_gearman: the daemon (a lithium console 
command named `gearmand`), and the utility class used to trigger jobs. 

Both tools require you to define a configuration, where you specify which 
Gearman servers are available. You do so by using the `config()` method of the 
`Gearman` class somewhere in your boostrap process. For example, to define a 
`default` configuration that uses a Gearman server located in th same server as
your lithium application, add the following code to 
`app/config/bootstrap/connections.php`:

```php
\li3_gearman\Gearman::config(array(
    'default' => array(
        'servers' => '127.0.0.1'
    )
));
```

Once you have a valid configuration, you can start the daemon, and start
triggering jobs.

## Running the daemon ##

The daemon is a lithium console command called `gearmand`. Running it without
arguments will show a message similar to this:

```text
USAGE
    li3 gearmand start [<config>]
    li3 gearmand stop
    li3 gearmand restart
DESCRIPTION
    Gearman daemon implementation in Lithium.
OPTIONS
    start
        Start the daemon using the given configuration.
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
from the console, is created to handle the workers), or in interactive mode. By
default it will run in interactive mode, but you can switch to deaemon mode
using the `--daemon` option.

Whatever method you decide to use, note that a certain amount of workers
(processes) are spawned. This number is governed by the option `--workers`. If
you wish to ensure that there is always a certain number of workers active
and handling jobs, use the option `--resucitate`, which will periodically loop
through the pool to respawn workers whenever other workers are finished, for 
any reason. You can also limit the total number of workers which are spawned
as a result of resucitation with the option `--limit`.

## Triggering jobs ##

Jobs can be triggered using the `Gearman::run()` method. This method takes the
following arguments:

* `configName`: The configuration name to use (see *Usage* section above)
* `action`: What action to execute. If using the default `Job` adapter that
comes bundled with li3\_gearman, then this should be a fully qualified method.
This means that it should consist of a fully qualified class name (with
namespaces), and a method name. If no method name is provided, the `run()`
method of the given class is assumed. Examples:

    * `app\tasks\Email::send`
    * `app\tasks\Caching`

* `args`: Arguments to pass to the action. First element in the array will
be the first argument. The second element will be the second argument, and so
on.
* `options`: Options that affect how the adapter triggers a job. If using
the default `Job` adapter, Available options are:

    * `background`: Wether to trigger the task in background. If set to `false`,
    then the task will be executed synchronously, and its result will be
    returned. If set to `true`, then a job handle will be returned. See
    [Gearman's doc on doBackground] [gearman-doc-dobackground] to learn
    more about job handles. Defaults to `true`.
    * `priority`: what priority to give the job. Can be any of `low`, `normal`,
    or `high`. Defaults to `normal`.

When jobs are triggered, one of the available workers (spawned by 
li3\_gearman's `gearmand` daemon) will handle and execute it from within the
console. This means that **any resource** available as part of lithium's 
dispatch cycle is immediately available to the job. So you can use your models, 
plugins, everything!

# Example #

Before starting with this example, make sure you have installed li3\_gearman
as clarified in the section *Installation*, and that you have added a default 
configuration as exemplified at the beginning of the section *Usage*.

Let us start by creating a task called `Hello`. Create a folder named `tasks`
and place it in your application's `app` folder. Inside that folder, create
a file named `Hello.php` with the following contents:

```php
<?php
namespace app\tasks;

class Hello {
    public static function run() {
        echo "I am " . __METHOD__ . "\n";
    }

    public static function say($name) {
        echo "I am " . __METHOD__ . "\n";
        return 'Hello ' . $name;
    }
}
?>
```

Now, let us add the code to trigger some tasks, both in background and
synchronous mode. Create a file named `TestHelloController.php` and place it in
your `app/controllers` folder with the following contents:

```php
<?php
namespace app\controllers;

use \li3_gearman\Gearman;

class TestHelloController extends \lithium\action\Controller {
    public function index() {
        $result = Gearman::run('default', 'app\tasks\Hello');
        var_dump($result);

        $result = Gearman::run('default', 'app\tasks\Hello::say', array(
            'Mariano'
        ), array('background' => false));
        var_dump($result);

        $this->_stop();
    }
}
?>
```

Ok we are now ready to start the daemon. For the purpose of testing (Gearman
0.26 has currently [some issues] [gearman-bug-800177] when running in non 
blocking mode on x86_64 architectures) I'll run this in blocking mode. Since we
also want some output, we'll ask the daemon to be verbose. Standing in your 
lithium core directory, run:

```bash
$ li3 gearmand start --verbose --blocking
```

You should see an output similar to the following (and the console should
not return to the prompt, since we are not running in daemon mode):

```text
li3_gearman[6331]: Daemon started with PID 6331
li3_gearman[6331]: (Daemon) Created worker number 1 with PID 6332
li3_gearman[6332]: (Worker) Starting worker
li3_gearman[6332]: (Worker) Creating Gearman worker
li3_gearman[6332]: (Worker) Registering function li3_gearman\extensions\command\Gearmand::run
li3_gearman[6331]: (Daemon) Created worker number 2 with PID 6333
li3_gearman[6333]: (Worker) Starting worker
li3_gearman[6333]: (Worker) Creating Gearman worker
li3_gearman[6333]: (Worker) Registering function li3_gearman\extensions\command\Gearmand::run
li3_gearman[6331]: (Daemon) Created worker number 3 with PID 6334
li3_gearman[6334]: (Worker) Starting worker
li3_gearman[6334]: (Worker) Creating Gearman worker
li3_gearman[6334]: (Worker) Registering function li3_gearman\extensions\command\Gearmand::run
li3_gearman[6331]: (Daemon) Created worker number 4 with PID 6335
li3_gearman[6335]: (Worker) Starting worker
li3_gearman[6335]: (Worker) Creating Gearman worker
li3_gearman[6335]: (Worker) Registering function li3_gearman\extensions\command\Gearmand::run
```

Now open up a browser, and access the controller we just created. If your
application lives in `localhost`, then browse to 
`http://localhost/test_hello`. You should see the following output on the
browser:

```text
string 'H:eternauta:27' (length=14)
string 'Hello Mariano' (length=13)
```

The first value shown there corresponds to the first job trigger, which
ran in background mode. This means that the given value is actually a job
handle. The second value corresponds to the job that was run in synchronous 
mode, so this is actually a value returned by the job itself!

If you switch to the console, you should see the following output as a result
of the controller action we just ran:

```text
li3_gearman[6473]: (Worker) Handling job
li3_gearman[6471]: (Worker) Handling job
I am app\tasks\Hello::run
I am app\tasks\Hello::say
```

[lithium]: http://lithify.me
[gearman]: http://gearman.org
[license]: http://www.opensource.org/licenses/bsd-license.php
[gearman-doc-dobackground]: http://docs.php.net/manual/en/gearmanclient.dobackground.php
[gearman-bug-800177]: https://bugs.launchpad.net/gearmand/+bug/800177
