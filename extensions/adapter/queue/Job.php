<?php

/**
 * Lithium: the most rad php framework
 *
 * @copyright     Copyright 2012, Mariano Iglesias (http://marianoiglesias.com.ar)
 * @license       http://opensource.org/licenses/bsd-license.php The BSD License
 */

namespace li3_gearman\extensions\adapter\queue;

use DateTime;
use DateTimeZone;
use Exception;
use InvalidArgumentException;
use RuntimeException;
use lithium\core\ConfigException;
use lithium\core\Environment;
use lithium\core\Object;
use GearmanClient;

/**
 * Executes/schedules a task
 */
class Job extends Object
{
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
    protected static $classes = [
        'worker' => 'li3_gearman\extensions\command\Gearmand'
    ];

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
    public function __construct(array $options)
    {
        parent::__construct($options);

        $this->client = new GearmanClient();
        foreach ($this->_config['servers'] as $server) {
            $this->client->addServer($server);
        }
    }

    /**
     * Get Gearman client
     *
     * @return object
     */
    public function getClient()
    {
        return $this->client;
    }

    /**
     * Run a task
     *
     * @param string $task Fully qualified class name, with optional method name
     * @param array $args Arguments to pass to the task
     * @param array $options Options:
     *                      - DateTime schedule: If specified, run at this time
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
    public function run($task, array $args, array $options)
    {
        $options += [
            'configName' => null,
            'background' => true,
            'unique' => false,
            'id' => null,
            'priority' => 'normal',
            'schedule' => null,
            'env' => array_intersect_key($_SERVER, [
                'HTTP_HOST' => null,
                'SCRIPT_FILENAME' => null,
                'SCRIPT_NAME' => null,
                'PHP_SELF' => null,
                'HTTPS' => null
            ]),
            'retries' => [],
            'retry' => 0
        ];

        if (!empty($options['retries'])) {
            if (!is_array($options['retries'])) {
                $options['retries'] = ['maximum' => $options['retries']];
            }
            $options['retries'] += [
                'maximum' => 5,
                'increment' => [
                    '5 minutes',
                    '10 minutes',
                    '20 minutes',
                    '30 minutes',
                    '50 minutes'
                ]
            ];

            $lastIncrement = (is_array($options['retries']['increment']) ? end($options['retries']['increment']) : $options['retries']['increment']);
            $increments = (is_array($options['retries']['increment']) ? count($options['retries']['increment']) : 0);
            if (!is_array($options['retries']['increment']) || $increments < $options['retries']['maximum']) {
                if (!is_array($options['retries']['increment'])) {
                    $options['retries']['increment'] = [];
                }
                $options['retries']['increment'] = array_merge($options['retries']['increment'], array_fill(0, $options['retries']['maximum'] - $increments, $lastIncrement));
            }
        }

        if (empty($options['configName'])) {
            throw new InvalidArgumentException('Missing configuration name');
        } elseif (!in_array($options['priority'], ['low', 'normal', 'high'])) {
            throw new InvalidArgumentException("Invalid priority {$options['priority']}");
        } elseif (isset($options['schedule'])) {
            if (!($options['schedule'] instanceof DateTime)) {
                throw new InvalidArgumentException('Invalid value specified for schedule option');
            } elseif (!$options['background']) {
                throw new InvalidArgumentException('Only background tasks can be scheduled');
            } elseif ($options['schedule']->getTimezone()->getName() !== 'UTC') {
                throw new InvalidArgumentException('Schedule time should be specified in UTC');
            }

            $now = new DateTime('now', new DateTimeZone('UTC'));
            if ($now >= $options['schedule']) {
                $options['schedule'] = null;
            }
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
            throw new InvalidArgumentException('Could not map priority to a gearman action');
        }

        $id = !empty($options['id']) ? $options['id'] : sha1(uniqid(time(), true));
        $env = $options['env'];
        $env['environment'] = Environment::get();
        $workload = [
            'id' => $id,
            'args' => $args,
            'env' => $env,
            'configName' => $options['configName'],
            'task' => $task,
            'background' => $options['background'],
            'retry' => $options['retry'],
            'retries' => $options['retries']
        ];

        if ($options['schedule']) {
            return $this->schedule($options['schedule'], $id, $action, $workload, $options);
        }
        return $this->queue($id, $action, $workload, $options);
    }

    /**
     * Process a scheduled task
     *
     * @return string Job ID
     * @filter
     */
    public function scheduled()
    {
        $config = $this->config('redis');
        if (empty($config['enabled'])) {
            throw new ConfigException('Can\'t process scheduled tasks without Redis support');
        }

        $now = new DateTime('now', new DateTimeZone('UTC'));
        $params = compact('now');
        return $this->_filter(
            __METHOD__,
            $params,
            function ($self, $params) {
                $config = $self->config('redis');
                $redis = $self->getRedis();
                $key = (!empty($config['schedulePrefix']) ? $config['schedulePrefix'] : 'job_scheduled');
                $redis->watch($key);

                $tasks = $redis->zRangeByScore($key, 0, $params['now']->getTimestamp(), ['limit' => [0, 1]]);
                if (!empty($tasks)) {
                    foreach ($tasks as $json) {
                        $redis->multi()->zrem($key, $json);
                        $result = $redis->exec();

                        $task = json_decode($json, true);
                        if (!empty($task) && is_array($task) && $result !== false) {
                            $self->queue($task['id'], $task['action'], $task['workload'], $task['options']);
                            return $task['id'];
                        }
                    }
                }

                $redis->unwatch();
            }
        );
    }

    /**
     * Schedule a Gearman task for later execution
     *
     * @param DateTime $when When to execute
     * @param string $id Job ID
     * @param string $action Gearman client method to use
     * @param array $workload Gearman workload
     * @param array $options Additional options for Gearman
     * @throws ConfigException
     * @filter
     */
    protected function schedule(DateTime $when, $id, $action, array $workload, array $options)
    {
        $config = $this->config('redis');
        if (empty($config['enabled'])) {
            throw new ConfigException('Can\'t schedule tasks without Redis support');
        }

        $params = [
            'when' => $when,
            'id' => $id,
            'json' => json_encode(compact('id', 'action', 'workload', 'options'))
        ];
        return $this->_filter(
            __METHOD__,
            $params,
            function ($self, $params) {
                $config = $self->config('redis');
                $redis = $self->getRedis();
                $key = (!empty($config['schedulePrefix']) ? $config['schedulePrefix'] : 'job_scheduled');
                $redis->zadd($key, $params['when']->getTimestamp(), $params['json']);
            }
        );
    }

    /**
     * Queues a Gearman task for execution
     *
     * @param string $id Job ID
     * @param string $action Gearman client method to use
     * @param array $workload Gearman workload
     * @param array $options Additional options for Gearman
     * @throws ConfigException
     */
    protected function queue($id, $action, array $workload, array $options)
    {
        $workload = json_encode($workload);
        if ($options['unique'] && !is_string($options['unique'])) {
            $options['unique'] = md5($workload);
        }

        try {
            $this->setStatus($id, static::STATUS_PENDING, $workload);
        } catch (Exception $e) {
        }

        return $this->client->{$action}(
            static::$classes['worker'] . '::run',
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
    public function execute($task, array $args = [], array $env = [], array $workload = [])
    {
        if (!is_callable($task)) {
            throw new RuntimeException("Invalid task {$task}");
        }

        $workload += ['id' => null, 'background' => false];

        try {
            $status = $this->getStatus($workload['id']);
        } catch (Exception $e) {
        }

        if (!empty($status) && $status != static::STATUS_PENDING) {
            throw new Exception("Job #{$workload['id']} not on pending status. Status: {$status}");
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
                call_user_func_array($this->_config['beforeExecute'], [$task, $args]);
            }

            $result = call_user_func_array($task, $args);

            $this->setStatus($workload['id'], static::STATUS_FINISHED);

            if (isset($this->_config['afterExecute']) && is_callable($this->_config['afterExecute'])) {
                call_user_func_array($this->_config['afterExecute'], [$task, $args]);
            }

        } catch (Exception $e) {
            error_log('[' . date('r') . '] ' . $e->getMessage());

            $this->setStatus($workload['id'], static::STATUS_ERROR);

            if (isset($this->_config['afterExecute']) && is_callable($this->_config['afterExecute'])) {
                call_user_func_array($this->_config['afterExecute'], [$task, $args]);
            }

            if (isset($this->_config['onException']) && is_callable($this->_config['onException'])) {
                call_user_func_array($this->_config['onException'], [$task, $args, $e]);
            }

            if (!empty($workload['retries']) && !empty($workload['retries']['maximum']) && $workload['retry'] <= $workload['retries']['maximum']) {
                $this->run($task, $args, [
                    'schedule' => new DateTime('now +' . $workload['retries']['increment'][$workload['retry']], new DateTimeZone('UTC')),
                    'retry' => $workload['retry'] + 1,
                    'retries' => $workload['retries']
                ] + array_intersect_key($workload, [
                    'configName' => null,
                    'env' => null,
                    'background' => null
                ]));
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
    protected function getStatus($id)
    {
        if (empty($id)) {
            return null;
        }

        $params = compact('id');
        return $this->_filter(
            __METHOD__,
            $params,
            function ($self, $params) {
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
    protected function setStatus($id, $status, $workload = [])
    {
        if (empty($id)) {
            return;
        } elseif (!in_array($status, [
            static::STATUS_ERROR,
            static::STATUS_FINISHED,
            static::STATUS_PENDING,
            static::STATUS_RUNNING
        ])) {
            throw new InvalidArgumentException("Invalid status {$status}");
        }

        $isFinished = ($status === static::STATUS_FINISHED);
        $isError = ($status === static::STATUS_ERROR);
        $params = compact('id', 'isError', 'isFinished', 'status', 'workload');
        return $this->_filter(
            __METHOD__,
            $params,
            function ($self, $params) {
                $config = $self->config('redis');
                if (empty($config['enabled']) || empty($config['saveJobStatus'])) {
                    return;
                }

                $redis = $self->getRedis();
                $key = (!empty($config['prefix']) ? $config['prefix'] : '') . $params['id'];
                $redis->set($key . '.status', $params['status']);
                if (!empty($params['workload'])) {
                    $redis->set($key . '.workload', $params['workload']);
                }

                // If setting as FINISHED, mark an expiration
                if ($params['isFinished'] && !empty($config['expires'])) {
                    if (extension_loaded('redis')) {
                        $redis->expire($key, $config['expires']);
                    } else {
                        $redis->keyExpire($key, $config['expires']);
                    }
                } elseif ($params['isError']) {
                    $redis->set($config['prefix'] . '.errors.' . $params['id'], $params['id']);
                }
            }
        );
    }

    /**
     * Get instance to Redis client
     *
     * @return object Redis client
     */
    public function getRedis()
    {
        if (!isset($this->redis)) {
            $config = $this->config('redis') + [
                'host' => '127.0.0.1',
                'port' => 6379
            ];

            if (extension_loaded('redis')) {
                $client = new \Redis();
                if (!$client->connect($config['host'], $config['port'])) {
                    throw new Exception("Could not connect to REDIS at {$config['host']}:{$config['port']}");
                }
            } elseif (class_exists('\Predis\Client')) {
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
    public function config($key = null)
    {
        if ($key === 'redis') {
            if (empty($this->_config['redis'])) {
                $this->_config['redis'] = [];
            }

            $this->_config['redis'] += [
                'enabled' => false,
                'saveJobStatus' => false,
                'prefix' => 'job.',
                'schedulePrefix' => 'job_scheduled',
                'beforeExecute' => null,
                'afterExecute' => null,
                'onException' => null,
                'expires' => 1 * 24 * 60 * 60 // 1 day
            ];
        }

        return isset($key) ? $this->_config[$key] : $this->_config;
    }
}