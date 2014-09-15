<?php

namespace Entity;

use Lib\Datastore\Api;

class Feed extends Api {

    protected $subscriber_url;
    
    protected $age;
    
    protected $name;

    public function getName() {
        return $this->name;
    }

    public function getAge() {
        return $this->age;
    }

    public function setName($name) {
        $this->name = $name;
    }

    public function setAge($age) {
        $this->age = $age;
    }

    public function setSubscriber_url($subscriber_url) {
        $this->subscriber_url = $subscriber_url;
    }

    public function getSubscriberUrl() {
        return $this->subscriber_url;
    }
}
