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
    private $PDO = null;
    private $config = [];

    //async
    private $mysqli;
    private $asyncQueryPool = [];

    /**
     * Default connection parameters
     *
     * @var array
     */
    protected $baseParams = [
        'driver' => 'mysql',
        'host' => 'localhost',
        'charset' => 'utf8',
        'collation' => 'utf8_unicode_ci',
        'prefix' => '',
    ];

    public function __construct(string $name = 'default', array $params = [])
    {
        if (self::$init === false) {
            try {
                parent::__construct();

                // Set the PDO connection
                // $this->getDatabaseManager()->extend('pdo_connection', function () use ($pdo) {
                //    return $pdo;
                // });

                $this->addConnection($name, $params);

                $this->setAsGlobal();
                $this->bootEloquent();

                //Class Aliases
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

    /**
     * Get Magento Config from the config file.
     *
     */
    public function getMageConfig()
    {
        $path = false;
        // if called stand alone before bootstrap BP is not avalable
        if (!defined("BP")) {
            if (file_exists(__DIR__ . '/../../../../app/etc/env.php')) {
                $path = realpath(__DIR__ . '/../../../..') . '/app/etc/env.php';
            } else if (file_exists(__DIR__ . '/../../../../../app/etc/env.php')) {
                $path = realpath(__DIR__ . '/../../../../..') . 'app/etc/env.php';
            } else {
                throw new \Exception("env file issue, not found");
            }
        } else {
            $path = \BP  . '/app/etc/env.php';
        }

        if ($path !== false) {
            return include $path;
        }
        return false;
    }

    /**
     * Create named connection - DB2 requres new connection
     *
     * @param string $name
     * @param array $params
     * @return void
     */
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

    /**
     * Init DB connection if not exists if used not from the new object()
     *
     * @param string $name - name of the magento connection
     * @param array $config - aditional parameters
     * @return void
     */
    public static function init(string $name = 'default', array $config = [])
    {
        if (static::$instance === null) {
            new self($name, $config);
        }
        return static::$instance;
    }

    /**
     * get DB connection if not exists if used not from the new object()
     *
     * @param string $name - name of the magento connection
     * @param array $config - aditional parameters
     * @return void
     */
    public static function instance(string $name = 'default', array $config = [])
    {
        if (static::$instance === null) {
            new self($name, $config);
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

    /**
     * Disable Enable Debug output
     *
     * @param boolean $on - enable true, disable false
     * @return void
     */
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

    /** Generate an SQL INSERT query from an associative array of data. * *
     * @param string $tableName
     * @param array $data
     * @return string
     */
    public static function generateInsertQuery($tableName, $data)
    {

        try {
            if (empty($data)) {
                throw new \Exception("Data list is empty.");
            }

            // Extract columns from the first data array
            $columns = array_keys($data[0]);

            // Escape column names for SQL
            $escapedColumns = array_map(function ($column) {
                return '`' . addslashes($column) . '`';
            }, $columns);

            // Create an array to hold value sets
            $valueSets = [];

            foreach ($data as $insert) {
                $values = array_values($insert);

                // Escape values for SQL
                $escapedValues = array_map(function ($value) {
                    if ($value === null) {
                        return 'NULL';
                    }
                    return '\'' . addslashes($value) . '\'';
                }, $values);

                $valueSets[] = '(' . implode(', ', $escapedValues) . ')';
            }

            // Create SQL query
            $columnsString = implode(', ', $escapedColumns);
            $valuesString = implode(', ', $valueSets);
            $sql = "INSERT INTO `{$tableName}` ({$columnsString}) VALUES {$valuesString};";

        } catch (\Throwable $e) {
            dump($data);
            echo $e->getMessage();
        }

        return $sql;

    }

    /**
     * Sanitize for RAW SQL using PDO function
     *
     * @param [type] $input
     * @return void
     */
    public function sanitize($input)
    {
        if (!$this->PDO) {
            $this->PDO = static::connection()->getPdo();
        }
        return $this->PDO->quote($input);
    }

    /**
     * Sanitize for RAW SQL alias
     * or use e() laravel function
     *
     * @param [type] $input
     * @return void
     */
    public function s($input)
    {
        return $this->sanitize($input);
    }
}
