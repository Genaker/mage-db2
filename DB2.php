<?php

namespace Mage\DB2;

use Illuminate\Container\Container;
use Illuminate\Database\Capsule\Manager as Capsule;
use Illuminate\Events\Dispatcher;

class DB2 extends Capsule
{
    static $instance = null;
    static $init = false;
    private $om;
    private $config = [];

    /**
     * @var array
     */
    protected $baseParams = [
        'driver' => 'mysql',
        'host' => 'localhost',
        'charset' => 'utf8',
        'collation' => 'utf8_unicode_ci',
        'prefix' => '',
    ];

    public function __construct(array $params = [])
    {
        if (self::$init === false) {
            try {
                parent::__construct();

                // Set the PDO connection
                // $this->getDatabaseManager()->extend('pdo_connection', function () use ($pdo) {
                //    return $pdo;
                // });

                $this->addConnection('default', $params);

                $this->setAsGlobal();
                $this->bootEloquent();
                class_alias(self::class, 'Mage\DB2');
                class_alias(self::class, 'DB2');

            } catch (\Exception $e) {
                throw new \Exception("DB2 init connection issue: " . $e->getMessage());
            }
            self::$init = true;
            static::$instance = $this;
        } else {
            return static::$instance;
            return $this;
        }
        return static::$instance;
    }

    private function getMageConfig()
    {
        $path = false;
        if (!defined("BP")) {
            if (file_exists(__DIR__ . '/../../../../app/etc/env.php')) {
                $path = realpath(__DIR__ . '/../../../..') . '/app/etc/env.php';
            } else if (file_exists(__DIR__ . '/../../../../../app/etc/env.php')) {
                $path = realpath(__DIR__ . '/../../../../..') . 'app/etc/env.php';
            } else {
                throw new \Exception("env file issue");
            }
        } else {
            $path = \BP  . '/app/etc/env.php';
        }

        if ($path !== false) {
            return include $path;
        }
        return false;
    }

    public function addConnection($name = 'default', $params = [])
    {
        $timeStart = microtime(false);
        $config = $this->getMageConfig();
        $dbConfig = $config['db']['connection'][$name];
        $dbConfig['database'] = $dbConfig['dbname'];
        $params = array_merge($dbConfig, $params);
        $params = array_merge($this->baseParams, $params);

        parent::addConnection($params);

        $this->getDatabaseManager()->setDefaultConnection('default');
        $timeEnd = microtime(false); //0.0001
    }

    /**
     * Get a connection instance from the global manager.
     *
     * @param  string|null  $connection
     * @return \Illuminate\Database\Connection
     */
    public static function connection($connection = null)
    {
        if (static::$instance === null) {
            static::init();
        }
        return static::$instance->getConnection($connection);
    }

    public static function init($config = [])
    {
        if (static::$instance === null) {
            new self;
        }
        return static::$instance;
    }

    /**
     * Get a fluent query builder instance.
     *
     * @param  \Closure|\Illuminate\Database\Query\Builder|string  $table
     * @param  string|null  $as
     * @param  string|null  $connection
     * @return \Illuminate\Database\Query\Builder
     */
    public static function table($table, $as = null, $connection = null)
    {
        static::init();
        return static::$instance->connection($connection)->table($table, $as);
    }

    /**
     * Get a schema builder instance.
     *
     * @param  string|null  $connection
     * @return \Illuminate\Database\Schema\Builder
     */
    public static function schema($connection = null)
    {
        static::init();
        return static::$instance->connection($connection)->getSchemaBuilder();
    }

    public static function debugOut($on = true)
    {

        if ($on) {
            static::connection()->setEventDispatcher(new Dispatcher(new Container));
            static::connection()->listen(function ($query) {
                echo "SQL: {$query->sql}\n";
                echo "Bindings: " . json_encode($query->bindings) . "\n";
                echo "Time: {$query->time}ms\n";
            });
        } else {
            static::connection()->setEventDispatcher(null);
            static::connection()->flushEventListeners();
        }
    }

    /**
     * Bind ? Parameters to SQL
     *
     * @param $query
     */
    public function toFullSql($query)
    {
        $sql = $query->toSql();
        $bindings = $query->getBindings();

        return vsprintf(
            str_replace('?', "'%s'", $sql),
            array_map('addslashes', $bindings)
        );
    }

    public function sendAsync(array $queries, $connection = 'default', $concurency = 5)
    {
        $config = $this->getMageConfig()['db']['connection'][$connection];
        $mysqli = [];
        //For now createing new connection for every Query. TODO: manage pool of the connections.
        $concurency = count($queries);
        // Create three separate MySQLi connections
        for ($i = 1; $i <= $concurency; $i++) {
            $mysqli[$i] = new mysqli($config['host'], $config["user"], $config["password"], $config["database"]);
            if ($mysqli[$i]->connect_errno) {
                die("Failed to connect to MySQL: " . $mysqli[$i]->connect_error);
            }
            $mysqli[$i]->query($queries[$i - 1], MYSQLI_ASYNC);
        }

        $links = $mysqli;

        // Process queries as results are ready
        do {
            $ready = mysqli_poll($links, $errors = [], $rejects = [], 1); // Wait for results (1-second timeout)
            if ($ready > 0) {
                foreach ($links as $key => $mysqli) {
                    if ($result = $mysqli->reap_async_query()) { // Fetch result
                        while ($row = $result->fetch_assoc()) {
                            echo $row['message'] . "\n";
                        }
                        $result->free();
                    } else {
                        //echo "Query failed: " . $mysqli->error . "\n";
                    }

                    // Remove completed connection
                    unset($links[$key]);
                }
            }
        } while (!empty($links)); // Continue until all queries are processed

        // Close connections
        foreach ($mysqli as $con) {
            $con->close();
        }
    }
}
