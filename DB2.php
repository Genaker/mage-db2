<?php

namespace Mage\DB2;

use Illuminate\Database\Capsule\Manager as Capsule;
use Illuminate\Events\Dispatcher;
use Illuminate\Container\Container;
use Mage\DotEnv as Env;

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
        if(!defined("BP")){
            if(file_exists(__DIR__ .'/../../../../app/etc/env.php')){
                $path = realpath(__DIR__ .'/../../../..') . '/app/etc/env.php';
            } else if(file_exists(__DIR__ .'/../../../../../app/etc/env.php')){
                $path = realpath(__DIR__ .'/../../../../..') . 'app/etc/env.php';
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
    public static function connection($connection = null){
        if(static::$instance === null){
            static::init();
        }
        return static::$instance->getConnection($connection);
    }

    public static function init($config = []) {
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

    public static function debugOut($on = true){

        if($on){
            static::connection()->setEventDispatcher(new Dispatcher(new Container));
            static::connection()->listen(function ($query){
                echo "SQL: {$query->sql}\n";
                echo "Bindings: " . json_encode($query->bindings) . "\n";
                echo "Time: {$query->time}ms\n";
            });
        } else {
            static::connection()->setEventDispatcher(null);
            static::connection()->flushEventListeners();

        }
    }
}


