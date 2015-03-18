<?php
class Model_Queue_Task extends Mango {

	protected $_collection = 'tasks';

	protected $_fields = array(
		// task execution data
        'status'      => array('type' => 'enum', 'values' => array('queued', 'active', 'failed', 'completed')),
		'created'     => array('type' => 'int'),
		'updated'     => array('type' => 'int'),
        'try'         => array('type' => 'int'),
        'message'     => array('type' => 'string'),

        // related minion task data
        'task'        => array('type' => 'string', 'required' =>TRUE),
        'options'     => array('type' => 'array'),

        // task settings
        'max_tries'   => array('type' => 'int', 'default' => 2),
        'keep_failed' => array('type' => 'boolean', 'default' => TRUE)
	);

	/**
	 * Finds and activates the next task to be executed
	 *
	 * @return   Model_Task   task (if not loaded, there is no next task)
	 */
	public function get_next()
	{
		$values = $this->db()->command( array(
			'findAndModify' => $this->_collection,
			'new'           => TRUE,
			'sort'          => array('created' => 1),
			'query'         => array('status' => array_search('queued', $this->_fields['status']['values'])),
			'update'        => array(
				'$inc' => array(
                    'try'  => 1
                ),
                '$set'    => array(
					'updated' => time(),
					'status'  => array_search('active', $this->_fields['status']['values']),
				)
			)
		));
		return Mango::factory('task', Arr::get($values,'value', array()), Mango::CLEAN);
	}

	public function create($options = array())
	{
		$this->values( array(
			'status'  => 'queued',
			'created' => time(),
            'try'     => 0
		));

		return parent::create($options);
	}

	public function update( $criteria = array(), $options = array())
	{
		$this->updated = time();

		return parent::update($criteria, $options);
	}

    public function execute()
    {
        // try
        $options = isset($this->options)
            ? $this->options->as_array(FALSE)
            : array();

        $options['task'] = $this->task;

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
                    ':id' => (string) $this->_id,
                    ':type' => (string) $this->task,
                    ':msg'  => $error
                ));

                // no need to retry
                $this->try = $this->max_tries;
                $this->message = $error;
            } else {
                $minion->execute();
                $success = TRUE;
            }
        } catch ( Exception $e) {
            $error = Kohana_Exception::text($e);

            Kohana::$log->add(Log::ERROR, 'Task :id failed on try: :try with message :msg', array(
                ':id'  => (string) $this->_id,
                ':msg' => $error,
                ':try' => $this->try
            ));

            $this->message = $error;
        }

        if ( $success) {
            // success, delete task
            $this->delete();
        } else {
            $this->status = 'failed';

            if ($this->try < $this->max_tries) {
                // requeue
                $this->status = 'queued';
                $this->update();
            } else if ( $this->keep_failed) {
                $this->update();
            } else {
                $this->delete();
            }
        }

        Kohana::$log->write();

        return $success;
    }
}