<?php

namespace Lib\Datastore;

final class Service {

    private static $instance = null;
    private static $required_options = [
        'datasetId',
        'applicationId',
    ];
    static $scopes = [
        "https://www.googleapis.com/auth/datastore",
        "https://www.googleapis.com/auth/userinfo.email",
    ];
    private static $callMappings = [
        'allocateIds' => '\Google_Service_Datastore_AllocateIdsRequest',
        'beginTransaction' => '\Google_Service_Datastore_BeginTransactionRequest',
        'commit' => '\Google_Service_Datastore_CommitRequest',
        'lookup' => '\Google_Service_Datastore_LookupRequest',
        'rollback' => '\Google_Service_Datastore_RollbackRequest',
        'runQuery' => '\Google_Service_Datastore_RunQueryRequest'
    ];
    private $dataset;
    private $datasetId;
    private $config = [];

    /**
     * @return DatastoreService The instance of the service.
     * @throws UnexpectedValueException
     */
    public static function getInstance() {
        if (self::$instance == null) {
            throw new \UnexpectedValueException('Instance has not been set.');
        }
        return self::$instance;
    }

    public static function setInstance($instance) {
        if (self::$instance != null) {
            throw new \UnexpectedValueException('Instance has already been set.');
        }
        self::$instance = $instance;
    }

    public function __construct($options) {
        $this->config = array_merge($this->config, $options);
        $this->init($this->config);
    }

    public function __call($name, $arguments) {
        if ((!array_key_exists($name, self::$callMappings)) || count($arguments) < 1) {
            throw new \Symfony\Component\Debug\Exception\UndefinedMethodException('Undefined method call :: ' . $name . '()', new \ErrorException());
        }
        
        if (is_a($arguments[0], self::$callMappings[$name])) {
            return call_user_func_array(array($this->dataset, $name), array_merge(array($this->datasetId), $arguments));
        } else {
            throw new \InvalidArgumentException();
        }
    }

    // Key helper function, abstracts the namespace
    public function createKey() {
        $key = new \Google_Service_Datastore_Key();

        if (isset($this->config['namespace'])) {
            $partition = new \Google_Service_Datastore_PartitionId();
            $partition->setNamespace($this->config['namespace']);
            $key->setPartitionId($partition);
        }

        return $key;
    }

    private function init($options) {
        foreach (self::$required_options as $requiredOptions) {
            if (!array_key_exists($requiredOptions, $options)) {
                throw new \InvalidArgumentException(
                'Option ' . $requiredOptions . ' must be supplied.');
            }
        }
        $client = new \Google_Client();
        $client->setApplicationName($options['applicationId']);
        
        $client->setAssertionCredentials(new \Google_Auth_AssertionCredentials($options['serviceAccountName'], 
                self::$scopes, file_get_contents($options['privateKey'])));
        
        $service = new \Google_Service_Datastore($client);

        $this->dataset = $service->datasets;
        $this->datasetId = $options['datasetId'];
    }

}
