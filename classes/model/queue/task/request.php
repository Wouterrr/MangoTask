<?php

// [!!] Request based tasks - use a request object or URI to execute internal and external requests

class Model_Queue_Task_Request extends Model_Task {

	protected $_type = 'request';

	protected function set_model_definition()
	{
		$this->_set_model_definition( array(
			'_fields' => array(
				'request'  => array('type' => 'string', 'filters' => array(array('serialize'))),
				'uri'      => array('type' => 'string'),
				'response' => array('type' => 'string')
			)
		));

		parent::set_model_definition();
	}

	/**
	 * Check if request is valid (/can be executed)
	 *
	 * @return   boolean   Request can be executed
	 */
	public function valid()
	{
		try
		{
			return $this->request() instanceof Request;
		}
		catch ( Exception $e)
		{
			// error during unserialization
			return FALSE;
		}
	}

	/**
	 * Execute request
	 *
	 * @return  boolean   Task was executed succesfully
	 */
	protected function _execute()
	{
		$request = $this->request();
		$error   = NULL;

		try
		{
			// execute task
			$response = $request->execute();

			// store response in task
			$this->response = $response->render();

			// analyse response
			if ( $response->status() > 199 && $response->status() < 300)
			{
				// task completed successfully
				return TRUE;
			}
			else
			{
				// server error
				$this->message = strtr("Invalid response status (:status) while executing :uri", array(
					':uri'      => $request->uri(),
					':status'   => $response->status(),
				));
			}
		}
		catch ( Exception $e)
		{
			// request error
			$this->message = strtr("Unable to execute task: :uri, (:msg)", array(
				':uri'     => $request->uri(),
				':msg'     => $e->getMessage(),
			));
		}

		return FALSE;
	}

	public function error_message($all = FALSE)
	{
		return ! $all
			? $this->message
			: strtr($this->message . '\n :request \n :response', array(
					':request'  => $this->request()->render(),
					':response' => $this->response
				));
	}

	/**
	 * Return request
	 *
	 * @return   Request   Request object
	 */
	public function request()
	{
		return isset($this->uri)
			? Request::factory($this->uri)
			: unserialize($this->request);
	}
}