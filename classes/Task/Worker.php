<?php

class Task_Worker extends Minion_Daemon {

    protected $_sleep = 1000000;

    protected $_break_on_exception = FALSE;

    public function loop(array $config)
    {
        try
        {
            // find next task
            $task = Mango::factory('task')->get_next();
        }
        catch ( MongoException $e)
        {
            Kohana::$log->add(Log::ERROR, Kohana_Exception::text($e));
            Kohana::$log->add(Log::ERROR, 'Error loading next task. Exiting');

            return FALSE;
        }

        if ( $task->loaded())
        {
            $task->execute();
        }

        return TRUE;
    }
}