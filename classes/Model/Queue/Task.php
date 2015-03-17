<?php
class Model_Queue_Task extends Mango {

	protected $_collection = 'tasks';

	protected $_fields = array(
		'message'  => array('type' => 'string'),
		'status'   => array('type' => 'enum', 'values' => array('queued', 'active', 'failed', 'completed')),
		'created'  => array('type' => 'int'),
		'updated'  => array('type' => 'int'),
        'task'     => array('type' => 'string', 'required' =>TRUE),
        'options'  => array('type' => 'array'),
        'try'      => array('type' => 'int')
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
}