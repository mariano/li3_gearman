<?php
/**
 * Lithium: the most rad php framework
 *
 * @copyright	  Copyright 2012, Mariano Iglesias (http://marianoiglesias.com.ar)
 * @license		  http://opensource.org/licenses/bsd-license.php The BSD License
 */

namespace li3_gearman\extensions\command;

use RuntimeException;
use GearmanException;
use GearmanJob;
use GearmanWorker;
use lithium\core\ConfigException;
use lithium\core\Environment;
use li3_gearman\Gearman;
use li3_gearman\extensions\adapter\queue\Job;

/**
 * Gearman daemon implementation in Lithium.
 */
class Gearmand extends \lithium\console\Command {
	/**
	 * Override environment to work on. Defaults to: environment set in bootstrap
	 *
	 * @var string
	 */
	public $environment;

	/**
	 * If enabled, once a worker has performed its job, it will quit and a new
	 * worker will be spawned (thus this setting means that `resuscitate` is
	 * automatically enabled, and `limit` set to 0). Default: enabled
	 *
	 * @var boolean
	 */
	public $atomic = false;

	/**
	 * Enable to interact with Gearman in blocking mode. Default: disabled
	 *
	 * @var boolean
	 */
	public $blocking = false;

	/**
	 * Enable to start daemon in a new process. Default: disabled
	 *
	 * @var boolean
	 */
	public $daemon = false;

	/**
	 * How many workers (in total) are allowed to be spawned before finishing
	 * daemon. Set to 0 to not limit spawned worker count. Default: 8
	 *
	 * @var int
	 */
	public $limit = 0;

	/**
	 * Location of PID file. Only applicable if daemon mode is enabled.
	 * Default: /var/run/li3_gearman.pid
	 *
	 * @var string
	 */
	public $pid = '/var/run/li3_gearman.pid';

	/**
	 * If enabled, there will always be the number of workers defined in the
	 * setting "workers". If a worker dies, another one will take its place,
	 * up until the "limit" setting is reached. If disabled, no new
	 * workers will be spawned after the initial set is started.
	 * Default: disabled
	 *
	 * @var boolean
	 */
	public $resuscitate = false;

	/**
	 * If enabled, log messages are sent to user's syslog (usually
	 * /var/log/user.log)
	 *
	 * @var boolean
	 */
	public $syslog = false;

	/**
	 * Enable to print out debug messages. If not enabled, messages go to
	 * user's syslog (usually /var/log/user.log). Default: disabled
	 *
	 * @var boolean
	 */
	public $verbose = false;

	/**
	 * How many workers to run. Default: 1
	 *
	 * @var int
	 */
	public $workers = 1;

	/**
	 * List of worker pids, and how many overall workers have been spawned
	 *
	 * @var array
	 */
	protected $_workers = array(
		'started' => 0,
		'pids' => array()
	);

	/**
	 * Process information
	 *
	 * @var array
	 */
	protected $_process = array(
		'run' => false,
		'reload' => false,
		'pid' => null,
		'daemonPid' => null,
		'isDaemon' => false,
		'isWorker' => false,
		'logOpened' => false,
		'exit' => 0
	);

	/**
	 * Configuration settings
	 *
	 * @var array
	 */
	protected $_settings;

	/**
	 * Start an individual worker
	 *
	 * @param string $config Gearman configuration name
	 */
	public function work($config = 'default') {
		$this->setup($config);
		$this->_process['run'] = true;
		$this->worker();
	}

	/**
	 * Test that at least a worker is working
	 */
	public function ping($config = 'default') {
		if ($this->verbose) {
			$this->out('Pinging... ', false);
		}
		$job = Gearman::adapter($config);
		$job->getClient()->setTimeout(5000);
		$result = @Gearman::run($config, 'li3_gearman\Gearman::ping', array(), array(
			'priority' => Job::PRIORITY_HIGH,
			'background' => false
		));
		if ($result) {
			$this->out($result);
			return;
		}
		$this->error('ERROR');
		$this->_stop(1);
	}

	/**
	 * Start the daemon using the given configuration.
	 *
	 * @param string $config Gearman configuration name
	 */
	public function start($config = 'default') {
		$this->setup($config);
		$this->init();

		foreach (array('posix_kill', 'pcntl_fork') as $function) {
			if (!function_exists($function)) {
				throw new ConfigException("Can't find function {$function}");
			}
		}

		if ($this->daemon) {
			if (!is_writable(dirname($this->pid))) {
				throw new ConfigException("Can't write PID to {$this->pid}");
			}

			if (file_exists($this->pid)) {
				$pid = intval(file_get_contents($this->pid));
				pcntl_waitpid($pid, $status, WNOHANG);
				if (posix_getsid($pid)) {
					throw new RuntimeException("Daemon already started with PID {$pid}");
				}
			}

			$pid = pcntl_fork();
			if ($pid === -1) {
				throw new RuntimeException('Could not spawn daemon');
			} elseif ($pid === 0) {
				if (posix_setsid() == -1) {
					throw new RuntimeException("Could not detach daemon from terminal");
				}

				$this->daemon();
			}

			file_put_contents($this->pid, $pid);
		} else {
			$pid = posix_getpid();
		}

		$this->log("Daemon started with PID {$pid}");

		if (!$this->daemon) {
			$this->daemon();
		}
	}

	/**
	 * Stop the daemon. Only applicable if started in daemon mode.
	 */
	public function shutdown() {
		$this->init();
		$this->log('Sending daemon the shutdown signal');
		$this->sendSignalToDaemon(SIGTERM);
	}

	/**
	 * Restart the daemon. Only applicable if started in daemon mode.
	 */
	public function restart() {
		$this->init();
		$this->log('Sending daemon the restart signal');
		$this->sendSignalToDaemon(SIGHUP);
	}

	/**
	 * Run the scheduler that allows for delayed/scheduled gearman tasks
	 *
	 * @param int $every How many seconds to wait between checks
	 * @param string $config Gearman configuration name
	 */
	public function scheduler($every = 60, $config = 'default') {
		if ($every <= 0) {
			$this->error('Invalid number of seconds');
			$this->_stop(1);
		}

		$this->log('Waiting for scheduled tasks');
		while(true) {
			$jobId = Gearman::scheduled($config);
			if (!empty($jobId)) {
				$this->log('Job #' . $jobId . ' moved for immediate execution');
			}
			sleep($every);
		}
	}

	/**
	 * Initialization and sanity checks
	 */
	protected function init() {
		declare(ticks = 30);

		$this->atomic = !empty($this->atomic);
		if ($this->atomic) {
			$this->resuscitate = true;
			$this->limit = 0;
		}

		foreach (array('posix_kill', 'pcntl_fork') as $function) {
			if (!function_exists($function)) {
				throw new ConfigException("Can't find function {$function}");
			}
		}
	}

	/**
	 * Setup.
	 *
	 * @param string $config Gearman configuration name
	 */
	protected function setup($config = 'default') {
		$this->_settings = Gearman::config($config);
		if (!isset($this->_settings)) {
			throw new ConfigException("{$config} is not a valid li3_gearman configuration");
		} elseif (empty($this->_settings['servers'])) {
			throw new ConfigException("{$config} defines no servers");
		}

		if (isset($this->environment)) {
			Environment::set($this->environment);
		} else {
			$this->environment = Environment::get();
			if (!$this->environment) {
				throw new ConfigException("Could not determine environment");
			}
		}
	}


	/**
	 * Daemon work loop. Starts the workers, and runs as long as told to.
	 */
	protected function daemon() {
		$this->_process['run'] = true;
		$this->_process['isDaemon'] = true;
		$this->_process['daemonPid'] = posix_getpid();
		$this->_process['pid'] = $this->_process['daemonPid'];

		foreach(array(SIGTERM, SIGHUP) as $signal) {
			if (!pcntl_signal($signal, array($this, '_signal'))) {
				throw new RuntimeException("Could not register signal {$signal}");
			}
		}

		$this->startWorkers();

		if (!pcntl_signal(SIGUSR1, array($this, '_signal'))) {
			throw new RuntimeException("Could not register signal SIGUSR1");
		}

		while ($this->_process['run']) {
			if ($this->_process['reload']) {
				$this->log('Restarting...', LOG_NOTICE);
				$this->_process['reload'] = false;
				$this->killWorkers();
				$this->startWorker();
			} else {
				$this->checkWorkers();
			}
			usleep(150000);
		}

		$this->log('Shutting down...', LOG_NOTICE);

		$this->killWorkers();

		if (file_exists($this->pid)) {
			unlink($this->pid);
		}

		$this->_stop($this->_process['exit']);
	}

	/**
	 * Handle work
	 *
	 * @param object $job Gearman job
	 * @return mixed Return value from job
	 */
	public function _work(GearmanJob $job) {
		try {
			$workload = $job->workload();
			if (empty($workload)) {
				throw new RuntimeException("No workload");
			}
			$params = @json_decode($workload, true);
			if (!$params || !is_array($params)) {
				throw new RuntimeException("Invalid workload: {$workload}");
			}

			$this->log('Handling job ' . (!empty($params['id']) ? "#{$params['id']} " : '') . $params['task'] . ' with arguments ' . json_encode($params['args']));

			$result = Gearman::execute(
				$params['configName'],
				$params['task'],
				$params['args'],
				!empty($params['env']) ? $params['env'] : array(),
				$params
			);
		} catch(\Exception $e) {
			$this->log('ERROR: ' . $e->getMessage(), LOG_ERR);
		}

		return isset($result) ? $result : null;
	}

	/**
	 * Worker
	 */
	protected function worker() {
		$this->log('Starting worker');

		$this->log('Creating Gearman worker');

		$worker = new GearmanWorker();
		if (!$this->blocking) {
			$worker->addOptions(GEARMAN_WORKER_NON_BLOCKING);
		}

		foreach($this->_settings['servers'] as $server) {
			$worker->addServer($server);
		}

		$this->log('Registering function ' . get_called_class() . '::run');
		$worker->addFunction(get_called_class() . '::run', array($this, '_work'));

		try {
			if (!$this->blocking) {
				while ($this->_process['run'] && (
					@$worker->work() ||
					$worker->returnCode() == GEARMAN_IO_WAIT ||
					$worker->returnCode() == GEARMAN_NO_JOBS
				)) {
					if ($worker->returnCode() == GEARMAN_SUCCESS) {
						if ($this->atomic) {
							$this->_process['run'] = false;
						}
						continue;
					}

					if (!@$worker->wait()) {
						if ($worker->returnCode() == GEARMAN_NO_ACTIVE_FDS) {
							$this->log('Got disconnected, so waiting for server...');
							sleep(5);
							continue;
						}
						break;
					}
				}
			} else {
				while($this->_process['run'] && $worker->work()) {
					if ($this->atomic) {
						$this->_process['run'] = false;
					} else {
						usleep(50000);
					}
				}
			}

			$worker->unregisterAll();
		} catch(GearmanException $e) {
			$this->log('ERROR: ' . $e->getMessage(), LOG_ERR);
			posix_kill($this->_process['daemonPid'], SIGUSR1);
		}

		$this->log('Worker finished');

		$this->_stop();
	}

	/**
	 * Spawn a new worker
	 *
	 * @param boolean $isRestart true if this worker started through a worker restart
	 */
	protected function startWorker($isRestart = false) {
		// We may have been called after a run change
		if (!$this->_process['run']) {
			return;
		}

		if (
			$this->resuscitate &&
			$this->limit > 0 &&
			$this->_workers['started'] >= $this->limit
		) {
			$this->log("Reached the maximum of {$this->limit} worker restarts", LOG_WARNING);
			$this->_process['run'] = false;
			return;
		}

		$pid = pcntl_fork();
		if ($pid === -1) {
			$this->error('Could not spawn worker');
		} elseif ($pid > 0) {
			$this->_workers['pids'][] = $pid;
			$this->_workers['started']++;
			$this->log("Created worker number {$this->_workers['started']} with PID {$pid}");
			return;
		}

		if (posix_setsid() == -1) {
			throw new RuntimeException("Could not detach worker from terminal");
		}

		$this->_process['isDaemon'] = false;
		$this->_process['isWorker'] = true;
		$this->_process['pid'] = posix_getpid();

		$this->worker();
	}

	/**
	 * Start the batch of workers
	 */
	protected function startWorkers() {
		if (!empty($this->_workers['pids'])) {
			$this->killWorkers();
		}

		$this->_workers['pids'] = array();
		for ($i=0; $i < $this->workers; $i++) {
			$this->startWorker();
		}
	}

	/**
	 * Kill all active workers
	 */
	protected function killWorkers() {
		foreach($this->_workers['pids'] as $pid) {
			$this->log("Shutting down worker {$pid}");
			posix_kill($pid, $this->blocking ? 9 : SIGTERM);
		}
		foreach($this->_workers['pids'] as $i => $pid) {
			pcntl_waitpid($pid, $status);
			unset($this->_workers['pids'][$i]);
		}
		$this->_workers['pids'] = array();
	}

	/**
	 * Check to see if there are defunct of gone workers. If so, spawn new
	 * workers to reach the maximum wanted
	 */
	protected function checkWorkers() {
		$valid = array();
		foreach($this->_workers['pids'] as $pid) {
			pcntl_waitpid($pid, $status, WNOHANG);
			if (posix_getsid($pid)) {
				$valid[] = $pid;
			}
		}
		$count = count($valid);
		if ($count > $this->workers) {
			for ($i = $count - 1; $i >= $this->workers; $i--) {
				posix_kill($valid[$i], SIGTERM);
			}
			for ($i = $count - 1; $i >= $this->workers; $i--) {
				pcntl_waitpid($valid[$i], $status);
			}
		} elseif ($this->resuscitate && $count < $this->workers) {
			for ($i = $count; $i < $this->workers; $i++) {
				$this->log('Replacing finished worker with a new one');
				$this->startWorker(true);
			}
		}
	}

	/**
	 * Send a signal to the daemon
	 *
	 * @param int $signal Signal
	 */
	protected function sendSignalToDaemon($signal) {
		if (!file_exists($this->pid)) {
			throw new RuntimeException("No PID found on {$this->pid}");
		}

		$pid = intval(file_get_contents($this->pid));
		pcntl_waitpid($pid, $status, WNOHANG);
		if (!posix_getsid($pid)) {
			throw new RuntimeException("Daemon with PID {$pid} seems to be gone. Delete the {$this->pid} file manually");
		}

		posix_kill($pid, $signal);
		pcntl_waitpid($pid, $status, WNOHANG);
	}

	/**
	 * Signal handler. Needs to be public
	 */
	public function _signal($signal) {
		switch($signal) {
			case SIGHUP:
				$this->_process['reload'] = true;
			break;
			case SIGUSR1:
				$this->log('Worker asked to abort startup process', LOG_WARNING);
				$this->_process['run'] = false;
				$this->_process['exit'] = 1;
			case SIGTERM:
				$this->_process['run'] = false;
			break;
		}
	}

	/**
	 * Send log message to syslog
	 *
	 * @param string $message Message
	 * @param int $level Log level
	 */
	protected function log($message, $level = LOG_DEBUG) {
		if (!$this->syslog && !$this->verbose) {
			return;
		}

		if ($this->syslog && !$this->_process['logOpened']) {
			$this->_process['logOpened'] = true;
			$options = LOG_PID;
			if ($this->verbose) {
				$options |= LOG_PERROR;
			}
			openlog("li3_gearman", $options, LOG_USER);
		}

		if ($this->_process['isDaemon']) {
			$actor = 'Daemon';
		} elseif ($this->_process['isWorker']) {
			$actor = 'Worker';
		}

		if (!empty($actor)) {
			$message = "({$actor}) {$message}";
		}

		if ($this->syslog) {
			syslog($level, $message);
		} else {
			$this->out($message);
		}
	}

	/**
	 * Writes a string to error stream.
	 *
	 * @param string $error The string to write.
	 * @param integer|string|array $options
	 *        integer as the number of new lines.
	 *        string as the style
	 *        array as :
	 *        - nl : number of new lines to add at the end
	 *        - style : the style name to wrap around the
	 * @return integer
	 */
	public function error($error = null, $options = array('nl' => 1)) {
		if (!empty($error)) {
			error_log($error);
		}
		return parent::error($error, $options);
	}

	/**
	 * Exit immediately. Primarily used for overrides during testing.
	 *
	 * @param integer|string $status integer range 0 to 254, string printed on exit
	 * @return void
	 */
	protected function _stop($status = 0) {
		if ($this->_process['logOpened']) {
			closelog();
			$this->_process['logOpened'] = false;
		}
		return parent::stop($status);
	}
}