<?php

class Task_Worker extends Minion_Daemon {

    protected $_sleep = 1000000;

    protected $_break_on_exception = FALSE;

    protected $_max_tries = 2;

    protected $_keep_failed = TRUE;

    public function before(array $config)
    {
        // Handle any setup tasks
    }

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
            $success = $this->_run_task($task);

            if ( $success) {
                // success, delete task
                $task->delete();
            } else {
                $task->status = 'failed';

                if ($task->try < $this->_max_tries) {
                    // requeue
                    $task->status = 'queued';
                    $task->update();
                } else if ( $this->_keep_failed) {
                    $task->update();
                } else {
                    $task->delete();
                }
            }

            Kohana::$log->write();
        }

        return TRUE;
    }

    protected function _run_task($task)
    {
        // try
        $options = isset($task->options)
            ? $task->options->as_array(FALSE)
            : array();

        $options['task'] = $task->task;

        $success = FALSE;

        try {
            // create minion task
            $minion = Minion_Task::factory($options);

            // validate options here, to handle invalid tasks
            $validation = Validation::factory($minion->get_options());
            $validation = $minion->build_validation($validation);

            if ( ! $validation->check()) {
                $errors = $validation->errors(TRUE);
                $error = array_shift($errors);

                // don't execute task, log validation error
                Kohana::$log->add(Log::ERROR, 'Task of id: :id and type: :type failed validation: ":msg"', array(
                    ':id' => (string) $task->_id,
                    ':type' => (string) $task->task,
                    ':msg'  => $error
                ));

                // no need to retry
                $task->try = $this->_max_tries;
                $task->message = $error;
            } else {
                $minion->execute();
                $success = TRUE;
            }
        } catch ( Exception $e) {
            $error = Kohana_Exception::text($e);

            Kohana::$log->add(Log::ERROR, 'Task :id failed on try: :try with message :msg', array(
                ':id'  => (string) $task->_id,
                ':msg' => $error,
                ':try' => $task->try
            ));

            $task->message = $error;
        }

        return $success;
    }

    public function after(array $config)
    {
        // Handle any cleanup tasks
    }
}