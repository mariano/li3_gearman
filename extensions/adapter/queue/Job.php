<?php

/**
 * Lithium: the most rad php framework
 *
 * @copyright	  Copyright 2012, Mariano Iglesias (http://marianoiglesias.com.ar)
 * @license		  http://opensource.org/licenses/bsd-license.php The BSD License
 */

namespace li3_gearman\extensions\adapter\queue;

use RuntimeException;
use lithium\core\ConfigException;
use lithium\core\Environment;
use GearmanClient;

/**
 * Executes/schedules a task
 */
class Job extends \lithium\core\Object {
	const PRIORITY_LOW = 'low';
	const PRIORITY_NORMAL = 'normal';
	const PRIORITY_HIGH = 'high';
	const STATUS_PENDING = 'pending';
	const STATUS_RUNNING = 'running';
	const STATUS_ERROR = 'error';
	const STATUS_FINISHED = 'finished';

	/**
	 * Defines fully qualified name of worker, which should implement
	 * a run() method and must be hooked to the gearman server
	 *
	 * @var array
	 */
	protected static $_classes = array(
		'worker' => 'li3_gearman\extensions\command\Gearmand'
	);

	/**
	 * Gearman client instance
	 *
	 * @var object
	 */
	protected $client;

	/**
	 * Redis client instance
	 *
	 * @var object
	 */
	protected $redis;

	/**
	 * Constructor
	 *
	 * @param array $options Options
	 */
	public function __construct(array $options) {
		parent::__construct($options);

		$this->client = new GearmanClient();
		foreach($this->_config['servers'] as $server) {
			$this->client->addServer($server);
		}
	}

	/**
	 * Get Gearman client
	 *
	 * @return object
	 */
	public function getClient() {
		return $this->client;
	}

	/**
	 * Run a task
	 *
	 * @param string $task Fully qualified class name, with optional method name
	 * @param array $args Arguments to pass to the task
	 * @param array $options Options:
	 *                      - boolean background: wether to run task in
	 *                      background
	 *                      - string id: Identifier for this task (auto generates
	 *                      one if none specified)
	 *                      - mixed unique: If true, generate a unique ID so two
	 *                      tasks with the same workload are considered equal.
	 *                      If string, use this unique ID for this task. If empty
	 *                      or false, do not treat this as a unique task.
	 *                      - string priority: Prority. One of: low, normal,
	 *                      high
	 *                      - array env: array of environment settings to set
	 *                      (from $_SERVER) for proper routing. If omitted,
	 *                      automatically set this.
	 * @return mixed If background, job handle. Otherwise job's return value
	 */
	public function run($task, array $args, array $options) {
		$options = $options + array(
			'configName' => null,
			'background' => true,
			'unique' => false,
			'id' => null,
			'priority' => 'normal',
			'env' => array_intersect_key($_SERVER, array(
				'HTTP_HOST' => null,
				'SCRIPT_FILENAME' => null,
				'SCRIPT_NAME' => null,
				'PHP_SELF' => null,
				'HTTPS' => null
			))
		);

		if (empty($options['configName'])) {
			throw new ConfigException('Missing configuration name');
		}

		if (!in_array($options['priority'], array('low', 'normal', 'high'))) {
			throw new ConfigException("Invalid priority {$options['priority']}");
		}

		if ($task[0] !== '\\') {
			$task = '\\' . $task;
		}
		if (strpos($task, '::') === false) {
			$task .= '::run';
		}

		$action = null;
		switch($options['priority']) {
			case 'low':
				$action = $options['background'] ? 'doLowBackground' : 'doLow';
			break;
			case 'normal':
				$action = $options['background'] ? 'doBackground' : (
					method_exists($this->client, 'doNormal') ? 'doNormal' : 'do'
				);
			break;
			case 'high':
				$action = $options['background'] ? 'doHighBackground' : 'doHigh';
			break;
		}
		if (empty($action)) {
			throw new Exception('Could not map to a gearman action');
		}

		$id = !empty($params['id']) ? $params['id'] : sha1(uniqid(time(), true));
		$env = $options['env'];
		$env['environment'] = Environment::get();
		$workload = json_encode(array(
			'id' => $id,
			'args' => $args,
			'env' => $env,
			'configName' => $options['configName'],
			'task' => $task,
			'background' => $options['background']
		));
		if ($options['unique'] && !is_string($options['unique'])) {
			$options['unique'] = md5($workload);
		}

		try {
			$this->setStatus($id, static::STATUS_PENDING, $workload);
		} catch(\Exception $e) { }

		return $this->client->{$action}(
			static::$_classes['worker'] . '::run',
			$workload,
			!empty($options['unique']) ? $options['unique'] : null
		);
	}

	/**
	 * Executes a given task with the given set of arguments.
	 * Called by li3_gearman deamon
	 *
	 * @param string $task Fully qualified task name
	 * @param array $args Arguments for the call
	 * @param array $env Environment settings to merge on $_SERVER
	 * @param array $workload Full workload
	 * @return mixed Returned value
	 */
	public function execute($task, array $args = array(), array $env = array(), array $workload = array()) {
		if (!is_callable($task)) {
			throw new RuntimeException("Invalid task {$task}");
		}

		$workload += array('id' => null, 'background' => false);

		try {
			$status = $this->getStatus($workload['id']);
		} catch(\Exception $e) { }

		if (!empty($status) && $status != static::STATUS_PENDING) {
			throw new \Exception("Job #{$workload['id']} not on pending status. Status: {$status}");
		}

		if (array_key_exists('environment', $env)) {
			Environment::set($env['environment']);
			unset($env['environment']);
		}

		if (!empty($env)) {
			$_SERVER = $env + $_SERVER;
		}

		$result = null;
		try {
			$this->setStatus($workload['id'], static::STATUS_RUNNING);

			if (isset($this->_config['beforeExecute']) && is_callable($this->_config['beforeExecute'])) {
				call_user_func_array($this->_config['beforeExecute'], array($task, $args));
			}

			$result = call_user_func_array($task, $args);

			$this->setStatus($workload['id'], static::STATUS_FINISHED);

			if (isset($this->_config['afterExecute']) && is_callable($this->_config['afterExecute'])) {
				call_user_func_array($this->_config['afterExecute'], array($task, $args));
			}

		} catch(\Exception $e) {
			error_log($e->getMessage());

			$this->setStatus($workload['id'], static::STATUS_ERROR);

			if (isset($this->_config['afterExecute']) && is_callable($this->_config['afterExecute'])) {
				call_user_func_array($this->_config['afterExecute'], array($task, $args));
			}

			throw $e;
		}

		return $result;
	}

	/**
	 * Get job status
	 *
	 * @param string $id Job ID (part of job payload)
	 * @return string Status, or null if none
	 * @filter
	 */
	protected function getStatus($id) {
		if (empty($id)) {
			return null;
		}

		$params = compact('id');
		return $this->_filter(__METHOD__, $params,
			function($self, $params) {
				$config = $self->config('redis');
				if (empty($config['enabled'])) {
					return null;
				}

				$redis = $self->getRedis();
				$key = (!empty($config['prefix']) ? $config['prefix'] : '') . $params['id'];
				return $redis->get($key . '.status');
			}
		);
	}

	/**
	 * Set job status
	 *
	 * @param string $id Job ID (part of job payload)
	 * @param string $status Status
	 * @param array $workload If specified, treat this as creation and save this workload
	 * @return string Status, or null if none
	 * @filter
	 */
	protected function setStatus($id, $status, $workload = array()) {
		if (empty($id)) {
			return;
		} else if (!in_array($status, array(
			static::STATUS_ERROR,
			static::STATUS_FINISHED,
			static::STATUS_PENDING,
			static::STATUS_RUNNING
		))) {
			throw new \InvalidArgumentException("Invalid status {$status}");
		}

		$isFinished = ($status === static::STATUS_FINISHED);
		$isError = ($status === static::STATUS_ERROR);
		$params = compact('id', 'isError', 'isFinished', 'status', 'workload');
		return $this->_filter(__METHOD__, $params,
			function($self, $params) {
				$config = $self->config('redis');
				if (empty($config['enabled'])) {
					return;
				}

				$redis = $self->getRedis();
				$key = (!empty($config['prefix']) ? $config['prefix'] : '') . $params['id'];
				$redis->set($key . '.status', $params['status']);
				if (!empty($params['workload'])) {
					$redis->set($key . '.workload', $params['workload']);
				}

				// If setting as FINISHED, mark an expiration
				if ($params['finished'] && !empty($config['expires'])) {
					if (extension_loaded('redis')) {
						$redis->expire($key, $config['expires']);
					} else {
						$redis->keyExpire($key, $config['expires']);
					}
				} else if ($params['isError']) {
					$redis->set($config['prefix'] . '.errors.' . $params['id']);
				}
			}
		);
	}

	public function getRedis() {
		$config = $this->config('redis');
		if (!isset($this->redis)) {
			$config += array(
				'host' => '127.0.0.1',
				'port' => 6379
			);

			if (extension_loaded('redis')) {
				$client = new \Redis();
				if (!$client->connect($config['host'], $config['port'])) {
					throw new \Exception("Could not connect to REDIS at {$config['host']}:{$config['port']}");
				}
			} else if (class_exists('\Predis\Client')) {
				$client = new \Predis\Client($config);
			}

			if (!isset($client)) {
				throw new ConfigException('No method to connect to Redis available');
			}

			$this->redis = $client;
		}

		return $this->redis;
	}

	/**
	 * Get a configuration variable / the whole configuration array
	 *
	 * @return mixed Either an array, or configuration value
	 */
	public function config($key = null) {
		if ($key === 'redis') {
			if (empty($this->_config['redis'])) {
				$this->_config['redis'] = array();
			}

			$this->_config['redis'] += array(
				'enabled' => false,
				'prefix' => 'job.',
				'beforeExecute' => null,
				'afterExecute' => null,
				'expires' => 1 * 24 * 60 * 60 // 1 day
			);
		}

		return isset($key) ? $this->_config[$key] : $this->_config;
	}
}

?>