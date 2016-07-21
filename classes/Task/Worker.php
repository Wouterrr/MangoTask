<?php

class Task_Worker extends Minion_Daemon {

    protected $_sleep = 1000000;

    protected $_break_on_exception = FALSE;

    protected $_auto_terminate = 100000000; // 100 mb

    protected $_max_tasks = 2;
    protected $_pids = array();

    public function __construct()
    {
        parent::__construct();

        pcntl_signal(SIGCHLD, array($this, 'handle_signals'));
    }

    public function handle_signals($signo)
    {
        switch ($signo)
        {
            case SIGCHLD:
                // Child died signal
                while( ($pid = pcntl_wait($status, WNOHANG || WUNTRACED)) > 0) {
                    // remove pid from list
                    unset($this->_pids[$pid]);
                }

                break;
            default:
                parent::handle_signals($signo);
                break;
        }
    }


    public function loop(array $config)
    {
        if ( $this->_auto_terminate && memory_get_usage() > $this->_auto_terminate) {
            Kohana::$log->add(Log::WARNING,"Queue autoterminating. Memory usage (:usage bytes) higher than limit (:limit bytes).",array(
                ":usage" => memory_get_usage(),
                ":limit" => $this->_auto_terminate
            ));
            return FALSE;
        } else if ( count($this->_pids) < $this->_max_tasks) {
            try {
                // find next task
                $task = Mango::factory('task')->get_next();
            } catch (MongoException $e) {
                Kohana::$log->add(Log::ERROR, Kohana_Exception::text($e));
                Kohana::$log->add(Log::ERROR, 'Error loading next task. Exiting');

                return FALSE;
            }

            if ($task->loaded()) {
                // Write log to prevent memory issues
                Kohana::$log->write();

                // close all database connections before forking
                foreach ( MangoDB::$instances as $instance) {
                    $instance->disconnect();
                }

                $pid = pcntl_fork();

                if ( $pid == -1) {
                    // error forking
                    Kohana::$log->add(Log::ERROR, 'Queue. Could not spawn child task process.');
                    Kohana::$log->write();
                    exit;
                } else if ( $pid) {
                    // parent process
                    $this->_pids[$pid] = time();
                } else {
                    // child process
                    $task->execute();
                    exit(0);
                }
            }
        }

        return TRUE;
    }

    public function after(array $config) {
        $tries = 0;

        while ( $tries++ < 5 && count($this->_pids)) {
            sleep(10);
            $this->kill_all();
        }

        if ( count($this->_pids)) {
            Kohana::$log->add(Log::ERROR,'Queue. Could not kill all children');
        }
    }

    protected function kill_all()
    {
        foreach ($this->_pids as $pid => $time)
        {
            posix_kill($pid, SIGTERM);
            usleep(1000);
        }
        return count($this->_pids) === 0;
    }
}