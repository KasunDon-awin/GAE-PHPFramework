<?php

namespace Lib\Datastore;

use Lib\Datastore\Service;

abstract class Api {

    const TYPE_DATE = 'date';

    protected $kindName;
    protected $keyId;
    protected $keyName;

    public function __construct() {
        $this->setKindName(str_replace("Entity\\", "", get_class($this)));
    }

    public function getKeyId() {
        return $this->keyId;
    }

    public function getKeyName() {
        return $this->keyName;
    }

    public function setKeyId($id) {
        $this->keyId = $id;
    }

    public function setKeyName($name) {
        $this->keyName = sha1($name);
    }

    protected function getKindProperties() {
        $propertyMap = [];

        foreach ($this->getPropertyList() as $variable => $value) {
            if ($this->{$variable} instanceof \DateTime) {
                $propertyMap[strtoupper($variable)] = $this->createProperty($this->{$variable}, true, 'date');
            } else {
                $propertyMap[strtoupper($variable)] = $this->createProperty($this->{$variable}, true);
            }
        }
        return $propertyMap;
    }

    protected function populate($results) {
        $class = "\\" . get_class($this);
        $class = new $class;
        foreach ($this->getPropertyList() as $variable => $value) {
            $class->{'set' . ucfirst($variable)}($results[strtoupper($variable)]->getStringValue());
        }
        $class->addMemory();
        return $class;
    }

    protected function getPropertyList() {
        $list = [];
        foreach (get_class_vars(get_class($this)) as $variable => $value) {
            if (strpos($variable, 'key') !== 0 && strpos($variable, 'kind') !== 0) {
                $list[$variable] = $value;
            }
        }
        return $list;
    }

    protected function extractResults($results) {
        $queryResults = [];
        foreach ($results as $result) {
            $entity = $this->populate($result['entity']['properties']);
            $entity->setKeyId(@$result['entity']['key']['path'][0]['id']);
            $entity->setKeyName(@$result['entity']['key']['path'][0]['name']);
            $queryResults[] = $entity;
        }
        return $queryResults;
    }

    protected function getCacheKey($input = null) {
        $key = (empty($input)) ?
                (empty($this->keyId)) ? $this->keyName :
                        $this->keyId : sha1($input);
        return sprintf("%s_%s", $this->getKindName(), $key);
    }

    public function setKindName($name) {
        $this->kindName = $name;
    }

    public function getKindName() {
        return $this->kindName;
    }

    public function put($txn = null) {
        $entity = $this->createEntity();

        $mutation = new \Google_Service_Datastore_Mutation();
        if ($this->keyId || $this->keyName) {
            $mutation->setUpsert([$entity]);
        } else {
            $mutation->setInsertAutoId([$entity]);
        }
        $req = new \Google_Service_Datastore_CommitRequest();
        if ($txn) {
            syslog(LOG_DEBUG, "doing put in transactional context $txn");
            $req->setTransaction($txn);
        } else {
            $req->setMode('NON_TRANSACTIONAL');
        }
        $req->setMutation($mutation);
        Service::getInstance()->commit($req);
        $this->addMemory();
    }

    public function fetchByName($key_name, $txn = null) {
        $path = new \Google_Service_Datastore_KeyPathElement();
        $path->setKind($this->getKindName());
        $path->setName($key_name);
        $key = Service::getInstance()->createKey();
        $key->setPath([$path]);
        return $this->fetch($key, $txn);
    }

    public function fetchById($key_id, $txn = null) {
        $path = new \Google_Service_Datastore_KeyPathElement();
        $path->setKind($this->getKindName());
        $path->setId($key_id);
        $key = Service::getInstance()->createKey();
        $key->setPath([$path]);
        return $this->fetch($key, $txn);
    }

    private function fetch($key, $txn = null) {
        $lookup_req = new \Google_Service_Datastore_LookupRequest();
        $lookup_req->setKeys([$key]);
        if ($txn) {
            // syslog(LOG_DEBUG, "fetching in transactional context $txn");
            $ros = new \Google_Service_Datastore_ReadOptions();
            $ros->setTransaction($txn);
            $lookup_req->setReadOptions($ros);
        }
        $response = Service::getInstance()->lookup($lookup_req);
        $found = $response->getFound();
        return $this->extractResults($found);
    }

    protected function createEntity() {
        $entity = new \Google_Service_Datastore_Entity();
        $entity->setKey($this->createKey($this));
        $entity->setProperties($this->getKindProperties());
        return $entity;
    }

    public function delete($txn = null) {
        if (empty($this->keyId) && empty($this->keyName)) {
            throw new \UnexpectedValueException("Can't delete entity; ID not defined.");
        }
        $this->clearMemory();
        $mutation = new \Google_Service_Datastore_Mutation();
        $mutation->setDelete([$this->createKey($this)]);
        $req = new \Google_Service_Datastore_CommitRequest();
        if ($txn) {
            syslog(LOG_DEBUG, "doing delete in transactional context $txn");
            $req->setTransaction($txn);
        } else {
            $req->setMode('NON_TRANSACTIONAL');
        }
        $req->setMutation($mutation);
        Service::getInstance()->commit($req);
    }

    public function all() {
        $query = $this->createQuery($this->getKindName());
        $results = $this->executeQuery($query);
        return $this->extractResults($results);
    }

    public function batchTxnMutate($txn, $batchput, $deletes = []) {
        if (!$txn) {
            throw new \UnexpectedValueException('Transaction value not set.');
        }
        $insert_auto_id_items = [];
        $upsert_items = [];
        $delete_items = [];
        foreach ($batchput as $item) {
            $entity = $item->createEntity();
            if ($item->keyId || $item->keyName) {
                $upsert_items[] = $entity;
            } else {
                $insert_auto_id_items[] = $entity;
            }
        }
        foreach ($deletes as $delitem) {
            $delitem->clearMemory();
            $delete_items[] = $this->createKey($delitem);
        }
        $mutation = new Google_Service_Datastore_Mutation();
        if (!empty($insert_auto_id_items)) {
            $mutation->setInsertAutoId($insert_auto_id_items);
        }
        if (!empty($upsert_items)) {
            $mutation->setUpsert($upsert_items);
        }
        if (!empty($delete_items)) {
            $mutation->setDelete($delete_items);
        }
        $req = new \Google_Service_Datastore_CommitRequest();
        $req->setMutation($mutation);
        $req->setTransaction($txn);
        // will throw Google_Service_Exception if there is contention
        Service::getInstance()->commit($req);
        // successful commit. Call the onItemWrite method on each of the batch put items
        foreach ($batchput as $item) {
            $item->addMemory();
        }
    }

    /**
     * Do a non-transactional batch put.  Split into sub-batches
     * if the list is too big.
     */
    public function putBatch($batchput) {
        $insert_auto_id_items = [];
        $upsert_items = [];
        $batch_limit = 490;
        $count = 0;

        // process the inserts/updates
        foreach ($batchput as $item) {
            $entity = $item->createEntity();

            if ($item->keyId || $item->keyName) {
                $upsert_items[] = $entity;
            } else {
                $insert_auto_id_items[] = $entity;
            }
            $count++;
            if ($count > $batch_limit) {
                // we've reached the batch limit-- write what we have so far
                $mutation = new \Google_Service_Datastore_Mutation();
                if (!empty($insert_auto_id_items)) {
                    $mutation->setInsertAutoId($insert_auto_id_items);
                }
                // TODO -- why was this an 'else'?
                // else if (!empty($upsert_items)) {
                if (!empty($upsert_items)) {
                    $mutation->setUpsert($upsert_items);
                }

                $req = new \Google_Service_Datastore_CommitRequest();
                $req->setMutation($mutation);
                $req->setMode('NON_TRANSACTIONAL');
                Service::getInstance()->commit($req);
                // reset the batch count and lists
                $count = 0;
                $insert_auto_id_items = [];
                $upsert_items = [];
            }
        }
        // insert the remainder.
        $mutation = new \Google_Service_Datastore_Mutation();
        syslog(LOG_DEBUG, "inserts " . count($insert_auto_id_items) . ", upserts " . count($upsert_items));
        if (!empty($insert_auto_id_items)) {
            $mutation->setInsertAutoId($insert_auto_id_items);
        }
        if (!empty($upsert_items)) {
            $mutation->setUpsert($upsert_items);
        }
        $req = null;
        $req = new \Google_Service_Datastore_CommitRequest();
        $req->setMutation($mutation);
        $req->setMode('NON_TRANSACTIONAL');
        Service::getInstance()->commit($req);

        //now, call the onItemWrite method on each of the batch put items
        foreach ($batchput as $item) {
            $item->addMemory();
        }
    }

    protected function addMemory() {
        $mc = new \Memcache();
        try {
            $key = $this->getCacheKey();
            $mc->add($key, $this, 0, 120);
        } catch (\Google_Cache_Exception $ex) {
            syslog(LOG_WARNING, "in onItemWrite: memcache exception");
        }
    }

    protected function clearMemory() {
        $mc = new Memcache();
        $key = $this->getCacheKey();
        $mc->delete($key);
    }

    protected function createProperty($input, $indexed = false, $type = null) {
        $prop = new \Google_Service_Datastore_Property();
        if (is_string($input)) {
            $prop->setStringValue($input);
            $prop->setIndexed($indexed);
        } elseif ($type == self::TYPE_DATE) {
            $date = new DateTime($input);
            $prop->setDateTimeValue($date->format(DateTime::ATOM));
            $prop->setIndexed($indexed);
        } else if (is_array($input)) {
            $values = [];
            foreach ($input as $s) {
                $value = new \Google_Service_Datastore_Value();
                $value->setStringValue($s);
                $value->setIndexed($indexed);
                $values[] = $value;
            }
            $prop->setListValue($values);
        }
        return $prop;
    }

    protected function createQuery($kind_name) {
        $query = new \Google_Service_Datastore_Query();
        $kind = new \Google_Service_Datastore_KindExpression();
        $kind->setName($kind_name);
        $query->setKinds([$kind]);
        return $query;
    }

    protected function executeQuery($query) {
        $req = new \Google_Service_Datastore_RunQueryRequest();
        $req->setQuery($query);
        $response = Service::getInstance()->runQuery($req);

        if (isset($response['batch']['entityResults'])) {
            return $response['batch']['entityResults'];
        }
        return [];
    }

    protected function createStringFilter($name, $value, $operator = 'equal') {
        $filter_value = new \Google_Service_Datastore_Value();
        $filter_value->setStringValue($value);
        $property_ref = new \Google_Service_Datastore_PropertyReference();
        $property_ref->setName($name);
        $property_filter = new \Google_Service_Datastore_PropertyFilter();
        $property_filter->setProperty($property_ref);
        $property_filter->setValue($filter_value);
        $property_filter->setOperator($operator);
        $filter = new \Google_Service_Datastore_Filter();
        $filter->setPropertyFilter($property_filter);
        return $filter;
    }

    protected function createDateFilter($name, $value, $operator = 'greaterThan') {
        $date_value = new \Google_Service_Datastore_Value();
        $date_time = new DateTime($value);
        $date_value->setDateTimeValue($date_time->format(DateTime::ATOM));
        $property_ref = new \Google_Service_Datastore_PropertyReference();
        $property_ref->setName($name);
        $property_filter = new \Google_Service_Datastore_PropertyFilter();
        $property_filter->setProperty($property_ref);
        $property_filter->setValue($date_value);
        $property_filter->setOperator($operator);
        $filter = new \Google_Service_Datastore_Filter();
        $filter->setPropertyFilter($property_filter);
        return $filter;
    }

    protected function orderBy($query, $name, $direction = 'descending') {
        $order = new \Google_Service_Datastore_PropertyOrder();
        $property_ref = new \Google_Service_Datastore_PropertyReference();
        $property_ref->setName($name);
        $order->setProperty($property_ref);
        $order->setDirection($direction);
        $query->setOrder([$order]);
    }

    protected function createCompositeFilter($filters) {
        $composite_filter = new \Google_Service_Datastore_CompositeFilter();
        $composite_filter->setOperator('and');
        $composite_filter->setFilters($filters);
        $filter = new \Google_Service_Datastore_Filter();
        $filter->setCompositeFilter($composite_filter);
        return $filter;
    }

    public function fetchBy($input, $type) {
        $mc = new \Memcache();
        $key = $this->getCacheKey($input);
        $response = $mc->get($key);
        if ($response) {
            return [$response];
        }

        $query = $this->createQuery($this->getKindName());
        $inputFilter = $this->createStringFilter($type, $input);
        $inputFilter = $this->createCompositeFilter([$inputFilter]);
        $query->setFilter($inputFilter);
        $results = $this->executeQuery($query);
        return $this->extractResults($results);
    }

    protected function createKey($item) {
        $path = new \Google_Service_Datastore_KeyPathElement();

        $path->setKind($item->getKindName());
        // Sanity check
        if (!empty($item->keyId) && !empty($item->keyName)) {
            throw new \UnexpectedValueException('Only one of key_id or key_name should be set.');
        }

        if (!empty($item->keyId)) {
            $path->setId($item->keyId);
        } else if (!empty($item->keyName)) {
            $path->setName($item->keyName);
        }

        $key = Service::getInstance()->createKey();
        $key->setPath([$path]);
        return $key;
    }

}
