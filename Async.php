<?php

namespace Mage\DB2;

use Mage\DB2\DB2 as DB;

class Async
{
    private $mysqli = null;
    private static $instances = [];
    private $asyncQueryPool = [];

    // Destructor to close the connection
    public function __destruct()
    {
        if ($this->mysqli) {
            $this->closeAsyncConnections();
        }
    }

    // Singleton instance getter
    public static function instance($name = 'default')
    {
        if (!isset(self::$instances[$name])) {
            self::$instances[$name] = new self();
        }
        return self::$instances[$name];
    }

    public function closeAsyncConnections()
    {
        // Close connections
        foreach ($this->mysqli as $connection) {
            $connection->close();
        }
    }

    public function connect($config)
    {
        return new \mysqli($config['host'], $config["username"], $config["password"], $config["dbname"]);
    }

    public function getConnections($concurrency = 5, $connection = "default")
    {
        $config = DB::init()->getMageConfig()['db']['connection'][$connection];
        // Create separate MySQLi connections
        for ($i = 0; $i < $concurrency; $i++) {
            if (!isset($this->mysqli[$i])) {
                $this->mysqli[$i] = $this->connect($config);
                if ($this->mysqli[$i]->connect_errno) {
                    error_log("Failed to connect to MySQL: " . $this->mysqli[$i]->connect_error);
                    throw new \Exception("Failed to connect to MySQL: " . $this->mysqli[$i]->connect_error);
                }
            }
        }
    }

    public function preconect($concurrency = 5, $connection = "default")
    {
        $this->getConnections($concurrency = 5, $connection = "default");
    }

    public function getConnection($thread, $connection = "default")
    {
        $i = $thread;
        if (!isset($this->mysqli[$i])) {
            $config = DB::init()->getMageConfig()['db']['connection'][$connection];
            $this->mysqli[$i] = $this->connect($config);
            if ($this->mysqli[$i]->connect_errno) {
                error_log("Failed to connect to MySQL: " . $this->mysqli[$i]->connect_error);
                throw new \Exception("Failed to connect to MySQL: " . $this->mysqli[$i]->connect_error);
            }
        }
        return $this->mysqli[$i];
    }

    /**
     * Send multiple SQL queries asyncroniously using MySQLi
     *
     * @param array $queries
     * @param integer $concurrency
     * @param string $connection
     * @return void
     */
    public function sendAsync(array $queries, $concurrency = 5, $connection = 'default', $timeout = 360, $debug = false, $await = true)
    {
        //$mysqli = [];
        if ($debug) {
            echo "sendAsync\n";
        }
        if (count($this->asyncQueryPool) > 0) {
            if ($debug) {
                print_r($this->asyncQueryPool);
            }
            $links[] = $errors[] = $rejects[] = $this->asyncQueryPool;
            $resultCount = \mysqli_poll($links, $errors, $rejects, 0, 500);
            $count = count($this->asyncQueryPool);
            if ($count > 0 && $count > $resultCount) {
                throw new \Exception("Only one Pool is avalable for one given instance of the async processor");
                return false;
            }
        }

        if (count($queries) > $concurrency) {
            throw new \Exception("Count of the SQL queries must be <= concurrency set concurrency to - " . count($queries) . " curent value " . $concurrency);
        }

        $this->getConnections($concurrency);

        // Send queries asynchronously
        foreach ($queries as $index => $query) {
            if ($debug) {
                echo "AddQuery $index\n";
            }

            //print_r($this->mysqli[$index]);
            // MYSQLI_ASYNC (available with mysqlnd) - the query is performed asynchronously and no result set is immediately returned.
            if (!$this->mysqli[$index]->query($query, MYSQLI_ASYNC)) {
                if ($debug) {
                    echo "Failed to send query: " . $this->mysqli[$index]->error . "\n";
                }
                error_log("Failed to send query: " . $this->mysqli[$index]->error);
            } else {
                $this->asyncQueryPool[$this->mysqli[$index]->thread_id] = $this->mysqli[$index];
            }
        }
        /*
        $links = $errors = $rejects = $this->asyncQueryPool;
        $ready = \mysqli_poll($links, $errors, $rejects, 0, 0);
        */

        if ($await) {
            if ($debug) {
                echo "Wait for await SQL\n";
            }
            $this->asyncAwait();
        }
        // Continue until all queries are processed
    }

    public function asyncAwait($debug = false)
    {
        $queryPool = &$this->asyncQueryPool;

        $links = $queryPool;
        $allLinks = $links;
        $checkCount = 0;
        //dd($links);

        //$links = array_filter($this->mysqli, function ($mysqli) { return $mysqli->stat(); });

        $errors = [];
        $rejects = [];
        $processed = 0;

        $return = [];

        // Process queries as results are ready
        if (count($links) > 0) {
            do {
                $checkCount++;
                if ($debug) {
                    echo "------[$processed]------\n";
                }
                // Initialize $links with active connections each loop
                $links = $allLinks;
                $errors = $allLinks;
                $rejects = $allLinks;

                $links = $errors = $rejects = array();
                foreach ($allLinks as $link) {
                    $links[] = $errors[] = $rejects[] = $link;
                }

                if (empty($links)) {
                    break; // Break the loop if there are no active connections
                }
                $ready = \mysqli_poll($links, $errors, $rejects, 0, 0); // Wait for results (0-second timeout)
                if ($ready > 0) {
                    if ($debug) {
                        echo "Ready: " . count($links);
                        echo "Error: " . count($errors);
                        echo "Rejected: " . count($rejects);
                    }
                    //die();
                }
                if ($ready === false) {
                    print_r($ready);
                    die("mysqli_poll ready error");
                }
                // $links will now only contain connections that are ready
                if ($debug) {
                    echo "Ready: ";
                    dump($ready);
                    echo "Errors: ";
                    dump($errors);
                    echo "Rejected: ";
                    dump($rejects);
                }

                // basicaly rejected when it is not select
                foreach ($rejects as $link) {
                    if ($debug) {
                        echo "Rejected->\n";
                        //print_r($link);
                        echo "Query rejected error: " . mysqli_error($link) . "\n";
                        echo "Affected Row Reject: " . $link->affected_rows . "\n";
                        if ($link->warning_count > 0) {
                            $result = $link->query("SHOW WARNINGS");
                            while ($row = $result->fetch_assoc()) {
                                echo "Level: " . $row['Level'] . "\n";
                                echo "Code: " . $row['Code'] . "\n";
                                echo "Message: " . $row['Message'] . "\n";
                            }
                            $result->close();
                        }
                    }

                    unset($queryPool[$link->thread_id]);
                    if ($ready === 0) {
                        $processed++;
                    } else {
                        // will be processed in the next move
                    }
                    //error_log("Query rejected: " . mysqli_error($link)); // Optionally, you can retry the query or handle it as needed
                }

                // basicaly rejected when it is not select
                foreach ($errors as $link) {
                    if ($debug) {
                        echo "Error->\n";
                        echo "Query Error: " . mysqli_error($link) . "\n";
                    }

                    unset($queryPool[$link->thread_id]);
                    if ($ready === 0) {
                        $processed++;
                    } else {
                        // will be processed in the next move
                    }
                    error_log("Async Query Error: " . mysqli_error($link)); // Optionally, you can retry the query or handle it as needed
                }

                if ($ready > 0) {
                    foreach ($links as $key => $mysqli) {
                        if ($debug) {
                            echo "check $key\n";
                        }

                        if ($result = $mysqli->reap_async_query()) { // Fetch result
                            if ($debug) {
                                print_r("Result $key: " . gettype($result) . "\n");
                            }
                            // For successful queries which produce a result set, such as SELECT, SHOW, DESCRIBE or EXPLAIN,
                            if ($result === true) {
                                $affectedRows = $mysqli->affected_rows;
                                if ($debug) {
                                    print_r("Affected Rows Insert: " . $affectedRows . "\n");
                                }
                                // Remove completed connection
                                // everething is ok on insert
                            }
                            if (is_object($result)) {
                                while ($row = $result->fetch_assoc()) {
                                    $return['rows'][] = $row;
                                }
                                if ($debug) {
                                    print_r("Rows Select:" . count($return) . " \n");
                                }
                                $result->free();
                                unset($queryPool[$mysqli->thread_id]);
                            }

                        } else {
                            if ($debug) {
                                echo "Query failed: " . $mysqli->error;
                            }
                            error_log("Query failed: " . $mysqli->error);
                        }
                        unset($queryPool[$mysqli->thread_id]);
                        $processed++;
                    }
                }
                \usleep(100);
            } while (!empty($queryPool) /*$processed < count($allLinks)*/);
            if ($debug) {
                echo "Loop: " . $checkCount . "\n";
            }
            //die();
        }
        if ($debug) {
            echo "Row Selected Count : " . count($return['rows']) . "\n";
        }
        return $return;
        //$this->asyncQueryPool = [];
    }
}
