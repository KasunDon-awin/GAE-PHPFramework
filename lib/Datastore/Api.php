<?php

namespace Lib\Datastore;

use Lib\Datastore\Service;

abstract class Api {

    const TYPE_DATE = 'date';

    protected $keyId = null;
    protected $keyName = null;

    protected function __construct() {
        
    }

    abstract protected function getKindProperties();

    protected static function getKindName() {
        throw new \RuntimeException("Unimplemented");
    }

    protected static function extractQueryResults($results) {
        throw new \RuntimeException("Unimplemented");
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
        $this->onItemWrite();
    }

    /**
     * Fetch an object from the datastore by key name.  Optionally indicate transaction.
     * @param $key_name
     * @param $txn
     */
    public static function fetchByName($key_name, $txn = null) {
        $path = new \Google_Service_Datastore_KeyPathElement();
        $path->setKind(static::getKindName());
        $path->setName($key_name);
        $key = Service::getInstance()->createKey();
        $key->setPath([$path]);
        return self::fetchByKey($key, $txn);
    }

    /**
     * Fetch an object from the datastore by key id.  Optionally indicate transaction.
     * @param $key_id
     * @param $txn
     */
    public static function fetchById($key_id, $txn = null) {
        $path = new \Google_Service_Datastore_KeyPathElement();
        $path->setKind(static::getKindName());
        $path->setId($key_id);
        $key = Service::getInstance()->createKey();
        $key->setPath([$path]);
        return self::fetchByKey($key, $txn);
    }

    /**
     * Fetch an object from the datastore by key.  Optionally indicate transaction.
     * @param $key
     * @param $txn
     */
    private static function fetchByKey($key, $txn = null) {
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
        $extracted = static::extractQueryResults($found);
        return $extracted;
    }

    protected function createEntity() {
        $entity = new \Google_Service_Datastore_Entity();
        $entity->setKey(self::createKeyForItem($this));
        $entity->setProperties($this->getKindProperties());
        return $entity;
    }

    /**
     * Delete a value from the datastore.
     * @throws UnexpectedValueException
     */
    public function delete($txn = null) {
        if (empty($this->keyId) && empty($this->keyName)) {
            throw new \UnexpectedValueException("Can't delete entity; ID not defined.");
        }
        $this->beforeItemDelete();
        $mutation = new \Google_Service_Datastore_Mutation();
        $mutation->setDelete([self::createKeyForItem($this)]);
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

    /**
     * Query the Datastore for all entities of this Kind
     */
    public static function all() {
        $query = self::createQuery(static::getKindName());
        $results = self::executeQuery($query);
        return static::extractQueryResults($results);
    }

    public static function batchTxnMutate($txn, $batchput, $deletes = []) {
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
            $delitem->beforeItemDelete();
            $delete_items[] = self::createKeyForItem($delitem);
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
            $item->onItemWrite();
        }
    }

    /**
     * Do a non-transactional batch put.  Split into sub-batches
     * if the list is too big.
     */
    public static function putBatch($batchput) {
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
            $item->onItemWrite();
        }
    }

    protected function setKeyId($id) {
        $this->keyId = $id;
    }

    protected function setKeyName($name) {
        $this->keyName = $name;
    }

    /**
     * Can be used by derived classes to update in-memory cache when an item is
     * written.
     */
    protected function onItemWrite() {
        
    }

    /**
     * Can be used by derived classes to delete from in-memory cache when an item is
     * deleted.
     */
    protected function beforeItemDelete() {
        
    }

    /**
     * Will create either string or list of strings property,
     * depending upon parameter passed.
     */
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

    /**
     * Create a query object for the given Kind.
     */
    protected static function createQuery($kind_name) {
        $query = new \Google_Service_Datastore_Query();
        $kind = new \Google_Service_Datastore_KindExpression();
        $kind->setName($kind_name);
        $query->setKinds([$kind]);
        return $query;
    }

    /**
     * Execute the given query and return the results.
     */
    protected static function executeQuery($query) {
        $req = new \Google_Service_Datastore_RunQueryRequest();
        $req->setQuery($query);
        $response = Service::getInstance()->runQuery($req);

        if (isset($response['batch']['entityResults'])) {
            return $response['batch']['entityResults'];
        } else {
            return [];
        }
    }

    /**
     * Create a query filter on a string property.
     * @param $name
     * @param $value
     * @param $operator
     */
    protected static function createStringFilter($name, $value, $operator = 'equal') {
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

    /**
     * Create a query filter on a date property.
     * @param $name property name
     * @param $value property value
     * @param $operator filter operator
     */
    protected static function createDateFilter($name, $value, $operator = 'greaterThan') {
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

    /**
     * Create a property 'order' and add it to a datastore query
     * @param $query the datastore query object
     * @param $name property name
     * @param $direction sort direction
     */
    protected static function addOrder($query, $name, $direction = 'descending') {
        $order = new \Google_Service_Datastore_PropertyOrder();
        $property_ref = new \Google_Service_Datastore_PropertyReference();
        $property_ref->setName($name);
        $order->setProperty($property_ref);
        $order->setDirection($direction);
        $query->setOrder([$order]);
    }

    /**
     * Create a composite 'and' filter.
     * @param $filters Array of filters
     */
    protected static function createCompositeFilter($filters) {
        $composite_filter = new \Google_Service_Datastore_CompositeFilter();
        $composite_filter->setOperator('and');
        $composite_filter->setFilters($filters);
        $filter = new \Google_Service_Datastore_Filter();
        $filter->setCompositeFilter($composite_filter);
        return $filter;
    }

    /**
     * Generate the Key for the item.
     * @param $item
     */
    protected static function createKeyForItem($item) {
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
