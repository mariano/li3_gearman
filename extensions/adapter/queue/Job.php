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
use GearmanClient;

/**
 * Executes/schedules a task
 */
class Job extends \lithium\core\Object {
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

		$env = $options['env'];
		$configName = $options['configName'];
		return $this->client->{$action}(
			static::$_classes['worker'] . '::run',
			json_encode(compact('args', 'env', 'configName', 'task'))
		);
	}

	/**
	 * Executes a given task with the given set of arguments.
	 * Called by li3_gearman deamon
	 *
	 * @param string $task Fully qualified task name
	 * @param array $args Arguments for the call
	 * @param array $env Environment settings to merge on $_SERVER
	 * @return mixed Returned value
	 */
	public function execute($task, array $args = array(), array $env = array()) {
		if (!is_callable($task)) {
			throw new RuntimeException("Invalid task {$task}");
		}

		if (!empty($env)) {
			$_SERVER = $env + $_SERVER;
		}

		return call_user_func_array($task, $args);
	}
}

?>
